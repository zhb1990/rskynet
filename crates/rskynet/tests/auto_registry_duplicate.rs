use rskynet::Registry;

#[derive(Default)]
struct First;

#[rskynet::service(name = "duplicate")]
impl First {}

#[derive(Default)]
struct Second;

#[rskynet::service(name = "duplicate")]
impl Second {}

#[test]
fn duplicate_auto_names_are_rejected() {
    let err = Registry::from_auto().expect_err("重名必须失败");
    let text = err.to_string();
    assert!(text.contains("`duplicate` 重复"), "实际错误：{text}");
    assert!(text.contains("First"), "错误应指出第一个类型：{text}");
    assert!(text.contains("Second"), "错误应指出第二个类型：{text}");
}
