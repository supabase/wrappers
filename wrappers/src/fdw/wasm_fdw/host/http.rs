use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Response, StatusCode,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

use super::super::bindings::supabase::wrappers::http;
use super::FdwHost;

// convert guest headers to HeaderMap
fn guest_to_header_map(headers: &http::Headers) -> HeaderMap {
    let mut header_map = HeaderMap::new();
    for (hdr, value) in headers {
        header_map.insert(
            HeaderName::from_lowercase(hdr.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    header_map
}

// convert HeaderMap to guest headers
fn header_map_to_guest(headers: &HeaderMap) -> http::Headers {
    headers
        .iter()
        .map(|(key, value)| {
            (
                key.as_str().to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

// create http request client with backoff retry
fn create_client(req: &http::Request) -> Result<ClientWithMiddleware, String> {
    let headers = guest_to_header_map(&req.headers);
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .gzip(true)
        .build()
        .map_err(|e| e.to_string())?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

impl FdwHost {
    // make a http request
    fn http_request(&mut self, req: http::Request) -> http::HttpResult {
        let client = create_client(&req)?;
        let resp = self
            .rt
            .block_on(
                match req.method {
                    http::Method::Get => client.get(req.url),
                    http::Method::Post => client.post(req.url),
                    http::Method::Put => client.put(req.url),
                    http::Method::Patch => client.patch(req.url),
                    http::Method::Delete => client.delete(req.url),
                }
                .body(req.body)
                .send(),
            )
            .map_err(|e| e.to_string())?;
        self.convert_to_guest_response(resp)
    }

    // convert reqwest response to guest response
    fn convert_to_guest_response(&mut self, resp: Response) -> http::HttpResult {
        let url = resp.url().to_string();
        let status_code = resp.status().as_u16();
        let headers = header_map_to_guest(resp.headers());
        let body = self.rt.block_on(resp.text()).map_err(|e| e.to_string())?;
        let resp = http::Response {
            url,
            status_code,
            headers,
            body,
        };
        Ok(resp)
    }
}

impl http::Host for FdwHost {
    #[inline]
    fn get(&mut self, req: http::Request) -> http::HttpResult {
        self.http_request(req)
    }

    #[inline]
    fn post(&mut self, req: http::Request) -> http::HttpResult {
        self.http_request(req)
    }

    #[inline]
    fn put(&mut self, req: http::Request) -> http::HttpResult {
        self.http_request(req)
    }

    #[inline]
    fn patch(&mut self, req: http::Request) -> http::HttpResult {
        self.http_request(req)
    }

    #[inline]
    fn delete(&mut self, req: http::Request) -> http::HttpResult {
        self.http_request(req)
    }

    fn error_for_status(&mut self, resp: http::Response) -> Result<(), http::HttpError> {
        let status = StatusCode::from_u16(resp.status_code).map_err(|e| e.to_string())?;
        if status.is_client_error() || status.is_server_error() {
            Err(format!(
                "HTTP status error ({}) for url ({})",
                status, resp.url
            ))
        } else {
            Ok(())
        }
    }
}
