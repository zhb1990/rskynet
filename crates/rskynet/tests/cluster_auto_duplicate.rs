use prost::Message as ProstMessage;
use rskynet::cluster::{ClusterError, HandlerRegistry, RemoteContext};

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 9001)]
struct Request {}

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 9002)]
struct Response {}

#[rskynet::cluster::handler("duplicate")]
async fn first(_remote: RemoteContext, _request: Request) -> std::result::Result<Response, String> {
    Ok(Response {})
}

#[rskynet::cluster::handler("duplicate")]
async fn second(
    _remote: RemoteContext,
    _request: Request,
) -> std::result::Result<Response, String> {
    Ok(Response {})
}

#[test]
fn duplicate_auto_handlers_are_rejected() {
    assert!(matches!(
        HandlerRegistry::from_auto(),
        Err(ClusterError::AutoRegistration { .. })
    ));
}
