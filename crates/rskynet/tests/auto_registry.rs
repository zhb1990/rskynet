use rskynet::{Registry, Result};

#[derive(Default)]
struct Shared;

#[rskynet::service(name = "auto-shared")]
impl Shared {}

struct Dedicated;

fn make_dedicated() -> Dedicated {
    Dedicated
}

#[rskynet::exclusive(name = "auto-dedicated", factory = make_dedicated)]
impl Dedicated {}

#[test]
fn named_macros_populate_the_auto_registry() -> Result<()> {
    let registry = Registry::from_auto()?;
    assert!(registry.contains("auto-shared"));
    assert!(registry.contains("auto-dedicated"));

    let mut kinds: Vec<_> = rskynet::__private::inventory::iter::<rskynet::AutoService>
        .into_iter()
        .map(|service| (service.name, service.exclusive))
        .collect();
    kinds.sort_unstable();
    assert_eq!(
        kinds,
        vec![
            ("auto-dedicated", true),
            ("auto-shared", false),
            ("bootstrap", false),
            ("logger", true),
            ("net", true),
        ]
    );
    Ok(())
}
