use crate::fdw::qdrant_fdw::qdrant_client::points::PointsResponseError::{ApiError, MissingResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct PointsRequest {
    limit: Option<u64>,
    offset: Option<u64>,
    with_payload: Option<bool>,
    with_vector: Option<bool>,
}

pub(crate) struct PointsRequestBuilder {
    request: PointsRequest,
}

impl PointsRequestBuilder {
    pub(crate) fn new() -> Self {
        Self {
            request: PointsRequest {
                limit: None,
                offset: None,
                with_payload: Some(false),
                with_vector: Some(false),
            },
        }
    }

    pub(crate) fn limit(mut self, limit: Option<u64>) -> Self {
        self.request.limit = limit;
        self
    }

    pub(crate) fn offset(mut self, offset: Option<u64>) -> Self {
        self.request.offset = offset;
        self
    }

    pub(crate) fn fetch_payload(mut self, fetch: bool) -> Self {
        self.request.with_payload = Some(fetch);
        self
    }

    pub(crate) fn fetch_vector(mut self, fetch: bool) -> Self {
        self.request.with_vector = Some(fetch);
        self
    }

    pub(crate) fn build(self) -> PointsRequest {
        self.request
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct Point {
    pub(crate) id: i64,
    pub(crate) payload: Option<serde_json::Value>,
    pub(crate) vector: Option<Vec<f32>>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct ResultPayload {
    pub(crate) points: Vec<Point>,
    pub(crate) next_page_offset: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct Success {
    status: String,
    result: ResultPayload,
    time: f64,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct PointsResponse {
    error: Option<String>,
    status: Option<String>,
    result: Option<ResultPayload>,
    time: Option<f64>,
}

#[derive(Error, Debug)]
pub(crate) enum PointsResponseError {
    #[error("api response error: {0}")]
    ApiError(String),

    #[error("{0}")]
    MissingResult(String),
}

impl PointsResponse {
    pub(crate) fn get_points_result(self) -> Result<ResultPayload, PointsResponseError> {
        if let Some(e) = self.error {
            return Err(ApiError(e));
        }

        let Some(result) = self.result else {
            return Err(MissingResult(
                "`result` key missing in response".to_string(),
            ));
        };

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use crate::fdw::qdrant_fdw::qdrant_client::points::{Point, PointsResponse, ResultPayload};
    use serde_json::json;

    #[test]
    fn response_deserialization_test_error() {
        let response_str = r#"
        {
          "error": "Not found: Collection `test_collection1` doesn't exist!"
        }
        "#;
        let response: PointsResponse = serde_json::from_str(response_str).unwrap();
        assert_eq!(
            response,
            PointsResponse {
                error: Some("Not found: Collection `test_collection1` doesn't exist!".to_string()),
                status: None,
                result: None,
                time: None,
            }
        )
    }

    #[test]
    fn response_deserialization_test_success() {
        let response_str = r#"
        {
          "result": {
            "points": [
              {
                "id": 1,
                "payload": {
                  "city": "Berlin"
                },
                "vector": [
                  0.05,
                  0.61,
                  0.76,
                  0.74
                ]
              },
              {
                "id": 2,
                "payload": {
                  "city": "London"
                },
                "vector": [
                  0.19,
                  0.81,
                  0.75,
                  0.11
                ]
              }
            ],
            "next_page_offset": 3
          },
          "status": "ok",
          "time": 0.001017542
        }        
        "#;
        let response: PointsResponse = serde_json::from_str(response_str).unwrap();
        assert_eq!(
            response,
            PointsResponse {
                error: None,
                status: Some("ok".to_string()),
                result: Some(ResultPayload {
                    points: vec![
                        Point {
                            id: 1,
                            payload: Some(json!({ "city": "Berlin" })),
                            vector: Some(vec![0.05, 0.61, 0.76, 0.74])
                        },
                        Point {
                            id: 2,
                            payload: Some(json!({ "city": "London" })),
                            vector: Some(vec![0.19, 0.81, 0.75, 0.11])
                        },
                    ],
                    next_page_offset: Some(3),
                }),
                time: Some(0.001017542),
            }
        )
    }
}
