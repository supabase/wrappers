use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    StatusCode,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

use super::super::bindings::v1::supabase::wrappers::http::{
    Headers as GuestHeaders, HttpError as GuestHttpError,
};
use super::FdwHost;

// convert guest headers to HeaderMap
fn guest_to_header_map(headers: &GuestHeaders) -> HeaderMap {
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
fn header_map_to_guest(headers: &HeaderMap) -> GuestHeaders {
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
fn create_client(headers: &GuestHeaders) -> Result<ClientWithMiddleware, GuestHttpError> {
    let headers = guest_to_header_map(headers);
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

// raise error for http status code
fn error_for_status(status_code: u16, url: &str) -> Result<(), GuestHttpError> {
    let status = StatusCode::from_u16(status_code).map_err(|e| e.to_string())?;
    if status.is_client_error() || status.is_server_error() {
        Err(format!("HTTP status error ({status}) for url ({url})"))
    } else {
        Ok(())
    }
}

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::http;

    impl FdwHost {
        // make a http request
        fn http_request(&mut self, req: http::Request) -> http::HttpResult {
            let client = create_client(&req.headers)?;
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

            let url = resp.url().to_string();
            let status_code = resp.status().as_u16();
            let headers = header_map_to_guest(resp.headers());
            let body = self.rt.block_on(resp.text()).map_err(|e| e.to_string())?;
            Ok(http::Response {
                url,
                status_code,
                headers,
                body,
            })
        }
    }

    impl http::Host for FdwHost {
        fn get(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req)
        }

        fn post(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req)
        }

        fn put(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req)
        }

        fn patch(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req)
        }

        fn delete(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req)
        }

        fn error_for_status(&mut self, resp: http::Response) -> Result<(), http::HttpError> {
            error_for_status(resp.status_code, &resp.url)
        }
    }
};

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::http as http_v1;
    use super::super::bindings::v2::supabase::wrappers::http;

    impl From<http::Method> for http_v1::Method {
        fn from(m: http::Method) -> Self {
            match m {
                http::Method::Get => http_v1::Method::Get,
                http::Method::Post => http_v1::Method::Post,
                http::Method::Put => http_v1::Method::Put,
                http::Method::Patch => http_v1::Method::Patch,
                http::Method::Delete => http_v1::Method::Delete,
            }
        }
    }

    impl From<http::Request> for http_v1::Request {
        fn from(r: http::Request) -> Self {
            Self {
                method: r.method.into(),
                url: r.url,
                headers: r.headers,
                body: r.body,
            }
        }
    }

    impl From<http_v1::Response> for http::Response {
        fn from(r: http_v1::Response) -> Self {
            Self {
                url: r.url,
                status_code: r.status_code,
                headers: r.headers,
                body: r.body,
            }
        }
    }

    impl http::Host for FdwHost {
        fn get(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req.into()).map(|r| r.into())
        }

        fn post(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req.into()).map(|r| r.into())
        }

        fn put(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req.into()).map(|r| r.into())
        }

        fn patch(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req.into()).map(|r| r.into())
        }

        fn delete(&mut self, req: http::Request) -> http::HttpResult {
            self.http_request(req.into()).map(|r| r.into())
        }

        fn error_for_status(&mut self, resp: http::Response) -> Result<(), http::HttpError> {
            error_for_status(resp.status_code, &resp.url)
        }
    }
};
