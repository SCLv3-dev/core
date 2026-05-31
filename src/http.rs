//! HTTP 包装，虽然是内部使用但是你也可以使用这个来做点 HTTP 请求什么的
//!
//! 或者在二次开发的时候更换成你喜欢的版本

use std::{sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use tokio::io::AsyncWriteExt;

use crate::prelude::*;

static GLOBAL_CLIENT: Lazy<Arc<reqwest::Client>> = Lazy::new(|| {
    let scl_version = std::option_env!("SCL_VERSION_TYPE").unwrap_or("0.0.0");
    let mut builder = reqwest::Client::builder()
        .user_agent(format!(
            "SharpCraftLauncher/{scl_version} (github.com/Steve-xmh/SharpCraftLauncher) (stevexmh@qq.com)"
        ))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(10))
        .pool_max_idle_per_host(1024);

    if let Ok(proxy) = std::env::var("HTTP_PROXY") {
        if let Ok(parsed) = reqwest::Proxy::all(&proxy) {
            tracing::trace!("Using http proxy: {}", proxy);
            builder = builder.proxy(parsed);
        }
    }

    let client = builder.build().expect("Failed to build HTTP client");
    Arc::new(client)
});

/// Future 重试调用函数，为下载文件失败重试而准备
pub async fn retry_future<O, F: std::future::Future<Output = O>>(
    max_retries: usize,
    future_builder: impl Fn() -> F,
    error_handler: impl Fn(&O) -> bool,
) -> DynResult<O> {
    let mut retries = 0;
    loop {
        retries += 1;
        let f = future_builder();
        let r = f.await;
        if error_handler(&r) || retries >= max_retries {
            return Ok(r);
        }
    }
}

/// 将 HTTP 响应流式写入文件
async fn stream_response_to_file(
    res: reqwest::Response,
    dest_path: &str,
) -> DynResult {
    let tmp_dest_path = format!("{dest_path}.tmp");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&tmp_dest_path)
        .await?;
    let mut stream = res.bytes_stream();
    use futures::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    drop(file);
    tokio::fs::rename(tmp_dest_path, dest_path).await?;
    Ok(())
}

/// 根据所给链接，依次尝试请求下载
///
/// 启发自 PCL1 源代码
///
/// TODO: 如 size 参数为非零值，则将会使用分片下载
pub async fn download(
    uris: &[impl AsRef<str> + std::fmt::Debug],
    dest_path: &str,
    _size: usize,
) -> DynResult {
    for uri in uris {
        let res = retry_future(5, || get(uri).send(), |r| r.is_ok()).await;
        match res {
            Ok(Ok(res)) => {
                if res.status().is_success() {
                    match stream_response_to_file(res, dest_path).await {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            tracing::trace!("Error {uri:?} 写入文件失败 {e}");
                        }
                    }
                } else {
                    tracing::trace!("Error {:?} 状态码错误 {}", uri, res.status());
                }
            }
            Ok(Err(e)) => {
                tracing::trace!("Error {uri:?} {e}")
            }
            Err(e) => {
                tracing::trace!("Error {uri:?} {e}")
            }
        }
    }
    anyhow::bail!(
        "轮询下载文件到 {} 失败，请检查你的网络连接，已尝试的链接 {:?}",
        dest_path,
        uris
    )
}

/// 重试获取 JSON 对象
///
/// 返回的数据结构需要实现 [`serde::de::DeserializeOwned`]
pub async fn retry_get_json<D: DeserializeOwned>(uri: impl AsRef<str>) -> DynResult<D> {
    let res = retry_future(
        5,
        || async { get(uri.as_ref()).send().await?.json::<D>().await },
        |r: &Result<D, reqwest::Error>| r.is_ok(),
    )
    .await?;
    res.map_err(|e| anyhow::anyhow!("轮询请求链接 {} 失败，请检查你的网络连接：{}", uri.as_ref(), e))
}

/// 重试获取数据
pub async fn retry_get_bytes(uri: impl AsRef<str>) -> DynResult<Vec<u8>> {
    let res = retry_future(
        5,
        || async {
            let b = get(uri.as_ref()).send().await?.bytes().await?;
            Ok::<Vec<u8>, reqwest::Error>(b.to_vec())
        },
        |r: &Result<Vec<u8>, reqwest::Error>| r.is_ok(),
    )
    .await?;
    res.map_err(|e| anyhow::anyhow!("轮询请求链接 {} 失败，请检查你的网络连接：{}", uri.as_ref(), e))
}

/// 重试获取字符串
pub async fn retry_get_string(uri: impl AsRef<str>) -> DynResult<String> {
    let res = retry_future(
        5,
        || async { get(uri.as_ref()).send().await?.text().await },
        |r: &Result<String, reqwest::Error>| r.is_ok(),
    )
    .await?;
    res.map_err(|e| anyhow::anyhow!("轮询请求链接 {} 失败，请检查你的网络连接：{}", uri.as_ref(), e))
}

/// 重试获取响应，当取得成功时返回
///
/// 你可能需要自行确认状态码是否成功
pub async fn retry_get(uri: impl AsRef<str>) -> DynResult<reqwest::Response> {
    let res = retry_future(5, || get(uri.as_ref()).send(), |r| r.is_ok()).await;
    let err = match res {
        Ok(Ok(body)) => return Ok(body),
        Ok(Err(e)) => anyhow::anyhow!("{}", e),
        Err(e) => e,
    };
    anyhow::bail!(
        "轮询请求链接 {} 失败，请检查你的网络连接：{}",
        uri.as_ref(),
        err
    )
}

/// 生成简单的 GET 请求
pub fn get(uri: impl AsRef<str>) -> reqwest::RequestBuilder {
    GLOBAL_CLIENT.get(uri.as_ref())
}

/// 生成简单的 POST 请求
pub fn post(uri: impl AsRef<str>) -> reqwest::RequestBuilder {
    GLOBAL_CLIENT.post(uri.as_ref())
}

/// 针对 Mojang 验证 API 的响应结构
#[derive(Debug, Clone)]
pub enum RequestResult<T> {
    /// 返回的结构是成功的，此处为实际数据
    Ok(T),
    /// 返回的结构是错误的，此处为错误信息结构
    Err(crate::auth::structs::mojang::ErrorResponse),
}

/// 不会进行重试的 HTTP 请求模块
pub mod no_retry {
    use serde::{de::DeserializeOwned, Serialize};

    use super::RequestResult;
    use crate::prelude::DynResult;

    /// 获取 JSON 对象
    ///
    /// 返回的数据结构需要实现 [`serde::de::DeserializeOwned`]
    pub async fn get_data<D: DeserializeOwned>(uri: &str) -> DynResult<RequestResult<D>> {
        let result = super::get(uri)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("无法接收来自 {} 的响应：{:?}", uri, e))?
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("无法接收来自 {} 的响应：{:?}", uri, e))?;
        if let Ok(result) = serde_json::from_str(&result) {
            Ok(RequestResult::Ok(result))
        } else {
            let result = serde_json::from_str(&result)?;
            Ok(RequestResult::Err(result))
        }
    }

    /// 带请求体去获取 JSON 对象
    ///
    /// 传入的请求体需要实现 [`serde::ser::Serialize`] 和 [`std::fmt::Debug`]
    ///
    /// 返回的数据结构需要实现 [`serde::de::DeserializeOwned`]
    pub async fn post_data<D: DeserializeOwned, S: Serialize + std::fmt::Debug>(
        uri: &str,
        body: &S,
    ) -> DynResult<RequestResult<D>> {
        let result = super::post(uri)
            .header("Content-Type", "application/json; charset=utf-8")
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("无法解析请求主体给 {}：{:?}", uri, e))?
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("无法接收来自 {} 的响应：{:?}", uri, e))?;
        if let Ok(result) = serde_json::from_str(&result) {
            Ok(RequestResult::Ok(result))
        } else {
            let result = serde_json::from_str(&result)?;
            Ok(RequestResult::Err(result))
        }
    }
}
