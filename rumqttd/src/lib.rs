#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{io, thread};
use std::{net::SocketAddr, sync::Arc};

use mqttbytes::v4::Packet;
use rumqttlog::*;
use tokio::time::error::Elapsed;

use crate::remotelink::RemoteLink;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::{task, time};

// All requirements for `rustls`
#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::{
    AllowAnyAuthenticatedClient, RootCertStore, ServerConfig, TLSError as RustlsError,
};

// All requirements for `native-tls`
#[cfg(feature = "use-native-tls")]
use std::io::Read;
#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls;
#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls::Error as NativeTlsError;
pub mod async_locallink;
mod consolelink;
mod locallink;
mod network;
mod remotelink;
mod state;

use crate::consolelink::ConsoleLink;
pub use crate::locallink::{LinkError, LinkRx, LinkTx};
use crate::network::Network;
#[cfg(feature = "use-rustls")]
use crate::Error::ServerKeyNotFound;
use std::collections::HashMap;

#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use std::fs::File;
#[cfg(feature = "use-rustls")]
use std::io::BufReader;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Connection error {0}")]
    Connection(#[from] remotelink::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[cfg(feature = "use-native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
    #[cfg(feature = "use-rustls")]
    #[error("Rustls error {0}")]
    Rustls(#[from] RustlsError),
    #[error("Server cert not provided")]
    ServerCertRequired,
    #[error("Server private key not provided")]
    ServerKeyRequired,
    #[error("CA file {0} no found")]
    CaFileNotFound(String),
    #[error("Server cert file {0} not found")]
    ServerCertNotFound(String),
    #[error("Server private key file {0} not found")]
    ServerKeyNotFound(String),
    #[error("Invalid CA cert file {0}")]
    InvalidCACert(String),
    #[error("Invalid server cert file {0}")]
    InvalidServerCert(String),
    #[error("Invalid server pass")]
    InvalidServerPass(),
    #[error("Invalid server key file {0}")]
    InvalidServerKey(String),
    RustlsNotEnabled,
    NativeTlsNotEnabled,
    Disconnected,
    NetworkClosed,
    WrongPacket(Packet),
}

type Id = usize;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: usize,
    pub router: rumqttlog::Config,
    pub servers: HashMap<String, ServerSettings>,
    pub cluster: Option<HashMap<String, MeshSettings>>,
    pub replicator: Option<ConnectionSettings>,
    pub console: ConsoleSettings,
}

#[allow(dead_code)]
enum ServerTLSAcceptor {
    #[cfg(feature = "use-rustls")]
    RustlsAcceptor { acceptor: tokio_rustls::TlsAcceptor },
    #[cfg(feature = "use-native-tls")]
    NativeTLSAcceptor {
        acceptor: tokio_native_tls::TlsAcceptor,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ServerCert {
    RustlsCert {
        ca_path: String,
        cert_path: String,
        key_path: String,
    },
    NativeTlsCert {
        pkcs12_path: String,
        pkcs12_pass: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub listen: SocketAddr,
    pub cert: Option<ServerCert>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionLoginCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_client_id_len: usize,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub max_inflight_size: usize,
    pub login_credentials: Option<Vec<ConnectionLoginCredentials>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshSettings {
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleSettings {
    pub listen: SocketAddr,
}

impl Default for ServerSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

impl Default for ConsoleSettings {
    fn default() -> Self {
        panic!("Console settings should be derived from configuration file")
    }
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(Id, Event)>,
    router: Option<Router>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let config = Arc::new(config);
        let router_config = Arc::new(config.router.clone());
        let (router, router_tx) = Router::new(router_config);
        Broker {
            config,
            router_tx,
            router: Some(router),
        }
    }

    pub fn router_handle(&self) -> Sender<(Id, Event)> {
        self.router_tx.clone()
    }

    pub fn link(&self, client_id: &str) -> Result<LinkTx, LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)

        let tx = LinkTx::new(client_id, self.router_tx.clone());
        Ok(tx)
    }

    pub fn start(&mut self) -> Result<(), Error> {
        // spawn the router in a separate thread
        let mut router = self.router.take().unwrap();
        let router_thread = thread::Builder::new().name("rumqttd-router".to_owned());
        router_thread.spawn(move || router.start())?;

        // spawn servers in a separate thread
        for (id, config) in self.config.servers.clone() {
            let server_name = format!("rumqttd-server-{}", id);
            let server_thread = thread::Builder::new().name(server_name);
            let server = Server::new(id, config, self.router_tx.clone());
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();
                runtime.block_on(async {
                    if let Err(e) = server.start().await {
                        error!("Stopping server. Accept loop error: {:?}", e.to_string());
                    }
                });
            })?;
        }

        let mut runtime = tokio::runtime::Builder::new_current_thread();
        let runtime = runtime.enable_all().build().unwrap();

        // Run console in current thread, if it is configured.
        let console = ConsoleLink::new(self.config.clone(), self.router_tx.clone());
        let console = Arc::new(console);
        runtime.block_on(async {
            consolelink::start(console).await;
        });

        Ok(())
    }
}

struct Server {
    id: String,
    config: ServerSettings,
    router_tx: Sender<(Id, Event)>,
}

impl Server {
    pub fn new(id: String, config: ServerSettings, router_tx: Sender<(Id, Event)>) -> Server {
        Server {
            id,
            config,
            router_tx,
        }
    }

    #[cfg(feature = "use-native-tls")]
    fn tls_native_tls(
        &self,
        pkcs12_path: &String,
        pkcs12_pass: &String,
    ) -> Result<Option<ServerTLSAcceptor>, Error> {
        // Get certificates
        let cert_file = File::open(&pkcs12_path);
        let mut cert_file =
            cert_file.map_err(|_| Error::ServerCertNotFound(pkcs12_path.clone()))?;

        // Read cert into memory
        let mut buf = Vec::new();
        cert_file
            .read_to_end(&mut buf)
            .map_err(|_| Error::InvalidServerCert(pkcs12_path.clone()))?;

        // Get the identity
        let identity = native_tls::Identity::from_pkcs12(&buf, &pkcs12_pass)
            .map_err(|_| Error::InvalidServerPass())?;

        // Builder
        let builder = native_tls::TlsAcceptor::builder(identity).build()?;

        // Create acceptor
        let acceptor = tokio_native_tls::TlsAcceptor::from(builder);
        Ok(Some(ServerTLSAcceptor::NativeTLSAcceptor { acceptor }))
    }

    #[allow(dead_code)]
    #[cfg(not(feature = "use-native-tls"))]
    fn tls_native_tls(
        &self,
        _pkcs12_path: &str,
        _pkcs12_pass: &str,
    ) -> Result<Option<ServerTLSAcceptor>, Error> {
        Err(Error::NativeTlsNotEnabled)
    }

    #[cfg(feature = "use-rustls")]
    fn tls_rustls(
        &self,
        cert_path: &str,
        key_path: &str,
        ca_path: &str,
    ) -> Result<Option<ServerTLSAcceptor>, Error> {
        let (certs, key) = {
            // Get certificates
            let cert_file = File::open(&cert_path);
            let cert_file =
                cert_file.map_err(|_| Error::ServerCertNotFound(cert_path.to_owned()))?;
            let certs = certs(&mut BufReader::new(cert_file));
            let certs = certs.map_err(|_| Error::InvalidServerCert(cert_path.to_string()))?;

            // Get private key
            let key_file = File::open(&key_path);
            let key_file = key_file.map_err(|_| ServerKeyNotFound(key_path.to_owned()))?;
            let keys = rsa_private_keys(&mut BufReader::new(key_file));
            let keys = keys.map_err(|_| Error::InvalidServerKey(key_path.to_owned()))?;

            // Get the first key
            let key = match keys.first() {
                Some(k) => k.clone(),
                None => return Err(Error::InvalidServerKey(key_path.to_owned())),
            };

            (certs, key)
        };

        // client authentication with a CA. CA isn't required otherwise
        let mut server_config = {
            let ca_file = File::open(ca_path);
            let ca_file = ca_file.map_err(|_| Error::CaFileNotFound(ca_path.to_owned()))?;
            let ca_file = &mut BufReader::new(ca_file);
            let mut store = RootCertStore::empty();
            let o = store.add_pem_file(ca_file);
            o.map_err(|_| Error::InvalidCACert(ca_path.to_string()))?;
            ServerConfig::new(AllowAnyAuthenticatedClient::new(store))
        };

        server_config.set_single_cert(certs, key)?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        Ok(Some(ServerTLSAcceptor::RustlsAcceptor { acceptor }))
    }

    #[allow(dead_code)]
    #[cfg(not(feature = "use-rustls"))]
    fn tls_rustls(
        &self,
        _cert_path: &String,
        _key_path: &String,
        _ca_path: &String,
    ) -> Result<Option<ServerTLSAcceptor>, Error> {
        Err(Error::RustlsNotEnabled)
    }

    async fn start(&self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count = 0;

        let config = Arc::new(self.config.connections.clone());

        // Get the ServerTLSAcceptor which allow us to use either Rustls or Native TLS
        #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
        let acceptor = match &self.config.cert {
            Some(c) => match c {
                ServerCert::RustlsCert {
                    ca_path,
                    cert_path,
                    key_path,
                } => self.tls_rustls(cert_path, key_path, ca_path)?,
                ServerCert::NativeTlsCert {
                    pkcs12_path,
                    pkcs12_pass,
                } => self.tls_native_tls(pkcs12_path, pkcs12_pass)?,
            },
            None => None,
        };

        let max_incoming_size = config.max_payload_size;

        info!(
            "Waiting for connections on {}. Server = {}",
            self.config.listen, self.id
        );
        loop {
            // Await new network connection.
            let (stream, addr) = match listener.accept().await {
                Ok((s, r)) => (s, r),
                Err(_e) => {
                    error!("Unable to accept socket.");
                    continue;
                }
            };

            // Depending on TLS or not create a new Network
            #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
            let network = match &acceptor {
                Some(a) => {
                    info!("{}. Accepting TLS connection from: {}", count, addr);

                    // Depending on which acceptor we're using address accordingly..
                    match a {
                        #[cfg(feature = "use-rustls")]
                        ServerTLSAcceptor::RustlsAcceptor { acceptor } => {
                            let stream = match acceptor.accept(stream).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Failed to accept TLS connection using Rustls. Error = {:?}", e);
                                    continue;
                                }
                            };

                            Network::new(stream, max_incoming_size)
                        }
                        #[cfg(feature = "use-native-tls")]
                        ServerTLSAcceptor::NativeTLSAcceptor { acceptor } => {
                            let stream = match acceptor.accept(stream).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Failed to accept TLS connection using Native TLS. Error = {:?}", e);
                                    continue;
                                }
                            };

                            Network::new(stream, max_incoming_size)
                        }
                    }
                }
                None => {
                    info!("{}. Accepting TCP connection from: {}", count, addr);
                    Network::new(stream, max_incoming_size)
                }
            };
            #[cfg(not(any(feature = "use-rustls", feature = "use-native-tls")))]
            let network = {
                info!("{}. Accepting TCP connection from: {}", count, addr);
                Network::new(stream, max_incoming_size)
            };

            count += 1;

            let config = config.clone();
            let router_tx = self.router_tx.clone();

            // Spawn a new thread to handle this connection.
            task::spawn(async {
                let connector = Connector::new(config, router_tx);
                if let Err(e) = connector.new_connection(network).await {
                    error!("Dropping link task!! Result = {:?}", e);
                }
            });

            time::sleep(delay).await;
        }
    }
}

struct Connector {
    config: Arc<ConnectionSettings>,
    router_tx: Sender<(Id, Event)>,
}

impl Connector {
    fn new(config: Arc<ConnectionSettings>, router_tx: Sender<(Id, Event)>) -> Connector {
        Connector { config, router_tx }
    }

    /// A new network connection should wait for mqtt connect packet. This handling should be handled
    /// asynchronously to avoid listener from not blocking new connections while this connection is
    /// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
    /// denial of service attacks (rogue clients which only does network connection without sending
    /// mqtt connection packet to make make the server reach its concurrent connection limit)
    async fn new_connection(&self, network: Network) -> Result<(), Error> {
        let config = self.config.clone();
        let router_tx = self.router_tx.clone();

        // Start the link
        let (client_id, id, mut link) = RemoteLink::new(config, router_tx, network).await?;
        let (execute_will, pending) = match link.start().await {
            // Connection get close. This shouldn't usually happen
            Ok(_) => {
                error!("Stopped!! Id = {} ({})", client_id, id);
                (true, link.state.clean())
            }
            // We are representing clean close as Abort in `Network`
            Err(remotelink::Error::Io(e)) if e.kind() == io::ErrorKind::ConnectionAborted => {
                info!("Closed!! Id = {} ({})", client_id, id);
                (true, link.state.clean())
            }
            // Client requested disconnection.
            Err(remotelink::Error::Disconnect) => {
                info!("Disconnected!! Id = {} ({})", client_id, id);
                (false, link.state.clean())
            }
            // Any other error
            Err(e) => {
                error!("Error!! Id = {} ({}), {}", client_id, id, e.to_string());
                (true, link.state.clean())
            }
        };

        let disconnect = Disconnection::new(client_id, execute_will, pending);
        let disconnect = Event::Disconnect(disconnect);
        let message = (id, disconnect);
        self.router_tx.send(message)?;
        Ok(())
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}
