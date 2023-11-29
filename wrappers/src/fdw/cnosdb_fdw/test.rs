#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::sql::Any;
    use arrow_flight::sql::CommandStatementQuery;
    use arrow_flight::{FlightDescriptor, FlightInfo};
    use pgrx::pg_test;
    use pgrx::prelude::*;
    use prost::Message;
    use tokio::runtime::Runtime;
    use tonic::codegen::http::header::AUTHORIZATION;
    use tonic::metadata::AsciiMetadataValue;
    use tonic::transport::Channel;
    use tonic::Request;
    use tonic::Response;

    fn build_request(cmd: CommandStatementQuery) -> Request<FlightDescriptor> {
        let pack = Any::pack(&cmd).expect("pack");

        let fd = FlightDescriptor::new_cmd(pack.encode_to_vec());

        let mut req = Request::new(fd);
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            AsciiMetadataValue::try_from(format!(
                "Basic {}",
                base64::encode(format!("{}:{}", "root", "")),
            ))
            .unwrap(),
        );
        req.metadata_mut()
            .insert("tenant", AsciiMetadataValue::try_from("cnosdb").unwrap());
        req.metadata_mut()
            .insert("db", AsciiMetadataValue::try_from("public").unwrap());
        req
    }

    fn handle_info(
        info: Response<FlightInfo>,
        client: &mut FlightServiceClient<Channel>,
        rt: &Runtime,
    ) {
        for ep in info.into_inner().endpoint {
            if let Some(ticket) = ep.ticket {
                rt.block_on(client.do_get(ticket)).unwrap();
            }
        }
    }

    fn write_prepare_data() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut client = rt
            .block_on(FlightServiceClient::connect("http://localhost:8904"))
            .expect("connect faile");
        let cmd1 = CommandStatementQuery {
            query: "CREATE TABLE air (
                    visibility DOUBLE,
                    temperature DOUBLE,
                    pressure DOUBLE,
                    TAGS(station));"
                .to_string(),
            transaction_id: None,
        };
        let cmd2 = CommandStatementQuery {
            query: "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63);".to_string(),
            transaction_id: None,
        };
        let cmd3 = CommandStatementQuery {
            query: "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:20:00', 'XiaoMai', 80, 60, 62);".to_string(),
            transaction_id: None,
        };
        let cmd4 = CommandStatementQuery {
            query: "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES('2023-01-01 01:30:00', 'Xiao', 81, 70, 61);".to_string(),
            transaction_id: None,
        };
        let req1 = build_request(cmd1);
        let info = rt.block_on(client.get_flight_info(req1)).unwrap();
        handle_info(info, &mut client, &rt);
        let req2 = build_request(cmd2);
        let info = rt.block_on(client.get_flight_info(req2)).unwrap();
        handle_info(info, &mut client, &rt);
        let req3 = build_request(cmd3);
        let info = rt.block_on(client.get_flight_info(req3)).unwrap();
        handle_info(info, &mut client, &rt);
        let req4 = build_request(cmd4);
        let info = rt.block_on(client.get_flight_info(req4)).unwrap();
        handle_info(info, &mut client, &rt);
    }

    #[pg_test]
    fn cnosdb_smoketest() {
        write_prepare_data();

        Spi::connect(|mut c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER cnosdb_wrapper
                         HANDLER cnosdb_fdw_handler VALIDATOR cnosdb_fdw_validator"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_cnosdb_server
                         FOREIGN DATA WRAPPER cnosdb_wrapper
                         OPTIONS (
                            url 'http://localhost:8904',
                            username 'root',
                            password '',
                            tenant 'cnosdb',
                            db 'public'
                         )"#,
                None,
                None,
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE air(
                    time timestamp,
                    station text,
                    visibility double precision,
                    temperature double precision,
                    pressure double precision
                  )
                  SERVER my_cnosdb_server
                  OPTIONS (
                    table 'air'
                  )
             "#,
                None,
                None,
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM air ORDER BY time", None, None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("station").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["XiaoMaiDao", "XiaoMai", "Xiao"]);

            let results = c
                .select("SELECT * FROM air ORDER BY temperature DESC", Some(1), None)
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("station").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["XiaoMaiDao"]);
        });
    }
}
