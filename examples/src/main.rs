mod cluster_ping_pong;
mod echo;
mod ping_pong;

fn main() -> std::process::ExitCode {
    rskynet::main::run()
}
