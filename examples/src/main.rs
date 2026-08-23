mod cluster_ping_pong;
mod debug_console;
mod echo;
mod http;
mod ping_pong;
mod quic;
mod websocket;

fn main() -> std::process::ExitCode {
    rskynet::main::run()
}
