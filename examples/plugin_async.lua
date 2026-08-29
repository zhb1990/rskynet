-- Rust 用 Function::call_async 驱动这个函数所在的 coroutine。
-- rskynet_sleep 是 Rust 通过 create_async_function 注入的异步函数：休眠期间
-- coroutine 会 yield，完成后再从这里继续执行。
return function(name, millis)
    rskynet_sleep(millis)
    return string.format("hello %s after %d ms", name, millis)
end
