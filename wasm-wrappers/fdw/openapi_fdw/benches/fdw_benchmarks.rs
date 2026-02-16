use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde_json::json;

// Note: We can't directly import from lib.rs due to WASM component model,
// so we'll benchmark the core algorithms that we can extract

/// Benchmark: camelCase to snake_case conversion (used for every column)
fn bench_sanitize_column_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_name_sanitization");

    let test_cases = vec![
        ("simpleCase", "simple_case"),
        ("clusterIP", "cluster_ip"),
        ("HTMLParser", "html_parser"),
        ("getHTTPSUrl", "get_https_url"),
        ("user_id", "user_id"),
        ("userId", "user_id"),
        ("@id", "_id"),
        ("created-at", "created_at"),
        ("123_start", "_123_start"),
    ];

    for (input, _expected) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(input), &input, |b, &input| {
            b.iter(|| {
                let mut result = String::new();
                let chars: Vec<char> = input.chars().collect();

                for (i, &c) in chars.iter().enumerate() {
                    if c.is_uppercase() && i > 0 {
                        let prev = chars[i - 1];
                        let next_is_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());

                        if prev.is_lowercase()
                            || prev.is_ascii_digit()
                            || (prev.is_uppercase() && next_is_lower)
                        {
                            result.push('_');
                        }
                        result.push(c.to_ascii_lowercase());
                    } else if c.is_alphanumeric() || c == '_' {
                        result.push(c.to_ascii_lowercase());
                    } else {
                        result.push('_');
                    }
                }

                if result.starts_with(|c: char| c.is_ascii_digit()) {
                    result.insert(0, '_');
                }

                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark: snake_case to camelCase (used during column matching)
fn bench_to_camel_case(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_camel_case");

    let test_cases = vec![
        "user_id",
        "created_at",
        "cluster_ip",
        "html_parser",
        "simple_name",
        "very_long_column_name_with_many_underscores",
    ];

    for input in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(input), &input, |b, &input| {
            b.iter(|| {
                let mut result = String::new();
                let mut capitalize_next = false;

                for c in input.chars() {
                    if c == '_' {
                        capitalize_next = true;
                    } else if capitalize_next {
                        result.push(c.to_uppercase().next().unwrap_or(c));
                        capitalize_next = false;
                    } else {
                        result.push(c);
                    }
                }

                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark: JSON object key lookup (happens once per cell)
fn bench_json_key_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_key_lookup");
    group.throughput(Throughput::Elements(1));

    // Small object (5 keys)
    let small_obj = json!({
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "createdAt": "2024-01-15T10:30:00Z",
        "isActive": true
    });

    // Medium object (20 keys)
    let medium_obj = json!({
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "createdAt": "2024-01-15T10:30:00Z",
        "updatedAt": "2024-01-15T10:30:00Z",
        "isActive": true,
        "age": 30,
        "city": "San Francisco",
        "country": "USA",
        "zipCode": "94102",
        "phoneNumber": "+1234567890",
        "companyName": "Acme Inc",
        "jobTitle": "Engineer",
        "department": "Engineering",
        "salary": 100000,
        "startDate": "2020-01-01",
        "manager": "Jane Smith",
        "teamSize": 5,
        "projects": ["project1", "project2"],
        "skills": ["rust", "sql"]
    });

    group.bench_function("small_object_exact_match", |b| {
        b.iter(|| {
            let obj = small_obj.as_object().unwrap();
            black_box(obj.get("name"))
        });
    });

    group.bench_function("medium_object_exact_match", |b| {
        b.iter(|| {
            let obj = medium_obj.as_object().unwrap();
            black_box(obj.get("name"))
        });
    });

    group.bench_function("small_object_case_insensitive", |b| {
        b.iter(|| {
            let obj = small_obj.as_object().unwrap();
            let target = "createdat";
            black_box(
                obj.iter()
                    .find(|(k, _)| k.to_lowercase() == target)
                    .map(|(_, v)| v),
            )
        });
    });

    group.finish();
}

/// Benchmark: DateTime normalization (happens for every date/timestamp cell)
fn bench_normalize_datetime(c: &mut Criterion) {
    let mut group = c.benchmark_group("normalize_datetime");

    group.bench_function("date_only", |b| {
        let input = "2024-01-15";
        b.iter(|| {
            let result = if input.len() == 10
                && input.as_bytes().get(4) == Some(&b'-')
                && input.as_bytes().get(7) == Some(&b'-')
            {
                format!("{input}T00:00:00Z")
            } else {
                input.to_string()
            };
            black_box(result)
        });
    });

    group.bench_function("full_datetime", |b| {
        let input = "2024-01-15T10:30:00Z";
        b.iter(|| {
            let result = if input.len() == 10
                && input.as_bytes().get(4) == Some(&b'-')
                && input.as_bytes().get(7) == Some(&b'-')
            {
                format!("{input}T00:00:00Z")
            } else {
                input.to_string()
            };
            black_box(result)
        });
    });

    group.bench_function("date_only_cow", |b| {
        use std::borrow::Cow;
        let input = "2024-01-15";
        b.iter(|| {
            let result: Cow<str> = if input.len() == 10
                && input.as_bytes().get(4) == Some(&b'-')
                && input.as_bytes().get(7) == Some(&b'-')
            {
                Cow::Owned(format!("{input}T00:00:00Z"))
            } else {
                Cow::Borrowed(input)
            };
            black_box(result)
        });
    });

    group.bench_function("full_datetime_cow", |b| {
        use std::borrow::Cow;
        let input = "2024-01-15T10:30:00Z";
        b.iter(|| {
            let result: Cow<str> = if input.len() == 10
                && input.as_bytes().get(4) == Some(&b'-')
                && input.as_bytes().get(7) == Some(&b'-')
            {
                Cow::Owned(format!("{input}T00:00:00Z"))
            } else {
                Cow::Borrowed(input)
            };
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: JSON parsing (happens once per page)
fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    // Small response (10 rows)
    let small_json = serde_json::to_string(&json!({
        "data": (0..10).map(|i| json!({
            "id": i,
            "name": format!("User {}", i),
            "email": format!("user{}@example.com", i),
            "createdAt": "2024-01-15T10:30:00Z",
            "isActive": true
        })).collect::<Vec<_>>()
    }))
    .unwrap();

    // Large response (1000 rows)
    let large_json = serde_json::to_string(&json!({
        "data": (0..1000).map(|i| json!({
            "id": i,
            "name": format!("User {}", i),
            "email": format!("user{}@example.com", i),
            "createdAt": "2024-01-15T10:30:00Z",
            "isActive": true
        })).collect::<Vec<_>>()
    }))
    .unwrap();

    group.throughput(Throughput::Bytes(small_json.len() as u64));
    group.bench_function("small_response_10_rows", |b| {
        b.iter(|| black_box(serde_json::from_str::<serde_json::Value>(&small_json).unwrap()));
    });

    group.throughput(Throughput::Bytes(large_json.len() as u64));
    group.bench_function("large_response_1000_rows", |b| {
        b.iter(|| black_box(serde_json::from_str::<serde_json::Value>(&large_json).unwrap()));
    });

    group.finish();
}

/// Benchmark: URL building with query parameters
fn bench_url_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("url_building");

    group.bench_function("no_params", |b| {
        let base = "https://api.example.com/users";
        let params: Vec<String> = vec![];
        b.iter(|| {
            let mut url = base.to_string();
            if !params.is_empty() {
                url.push('?');
                url.push_str(&params.join("&"));
            }
            black_box(url)
        });
    });

    group.bench_function("few_params_3", |b| {
        let base = "https://api.example.com/users";
        let params = vec![
            "limit=100".to_string(),
            "offset=0".to_string(),
            "sort=created_at".to_string(),
        ];
        b.iter(|| {
            let mut url = base.to_string();
            if !params.is_empty() {
                url.push('?');
                url.push_str(&params.join("&"));
            }
            black_box(url)
        });
    });

    group.bench_function("many_params_10", |b| {
        let base = "https://api.example.com/users";
        let params = vec![
            "limit=100".to_string(),
            "offset=0".to_string(),
            "sort=created_at".to_string(),
            "filter=active".to_string(),
            "include=profile".to_string(),
            "fields=id,name,email".to_string(),
            "page=1".to_string(),
            "per_page=50".to_string(),
            "order=desc".to_string(),
            "search=test".to_string(),
        ];
        b.iter(|| {
            let mut url = base.to_string();
            if !params.is_empty() {
                url.push('?');
                url.push_str(&params.join("&"));
            }
            black_box(url)
        });
    });

    group.finish();
}

/// Benchmark: Type conversion (happens for every cell)
fn bench_type_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_conversion");

    group.bench_function("json_to_i64", |b| {
        let val = json!(12345);
        b.iter(|| black_box(val.as_i64()));
    });

    group.bench_function("json_to_f64", |b| {
        let val = json!(123.45);
        b.iter(|| black_box(val.as_f64()));
    });

    group.bench_function("json_to_string", |b| {
        let val = json!("test string");
        b.iter(|| black_box(val.as_str().map(|s| s.to_owned())));
    });

    group.bench_function("json_to_bool", |b| {
        let val = json!(true);
        b.iter(|| black_box(val.as_bool()));
    });

    group.bench_function("json_complex_to_string", |b| {
        let val = json!({
            "nested": {
                "object": "value"
            }
        });
        b.iter(|| black_box(val.to_string()));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sanitize_column_name,
    bench_to_camel_case,
    bench_json_key_lookup,
    bench_normalize_datetime,
    bench_json_parsing,
    bench_url_building,
    bench_type_conversion,
);

criterion_main!(benches);
