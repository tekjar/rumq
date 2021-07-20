use argh::FromArgs;
use librumqttd::{Broker, Config};
use std::path::PathBuf;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(FromArgs, Debug)]
/// Command line args for rumqttd
struct CommandLine {
    /// path to config file
    #[argh(
        option,
        short = 'c',
        default = "PathBuf::from(\"config/rumqttd.conf\")"
    )]
    config: PathBuf,
    #[cfg(feature = "prof")]
    /// enable profiling
    #[argh(switch, short = 'p')]
    profile: bool,
}

fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(&commandline.config).unwrap();

    #[cfg(feature = "prof")]
    let _guard = prof::new(commandline.profile);

    let o = Broker::new(config).start();
    println!("Stopping broker!! Error = {:?}", o);
}

#[cfg(feature = "prof")]
mod prof {
    use pprof::protos::Message;
    pub struct Profile(pprof::ProfilerGuard<'static>);
    pub fn new(enable: bool) -> Option<Profile> {
        if enable {
            Some(Profile(pprof::ProfilerGuard::new(100).unwrap()))
        } else {
            None
        }
    }
    impl Drop for Profile {
        fn drop(&mut self) {
            if let Ok(report) = self.0.report().build() {
                let profile = report.pprof().unwrap();
                let mut buf = Vec::with_capacity(profile.encoded_len());
                profile.encode(&mut buf).unwrap();
                let path = "rumqttd.pb";
                std::fs::write(path, &buf).unwrap();
                println!("wrote profile to {}", path);
            }
        }
    }
}
