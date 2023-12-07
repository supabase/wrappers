impl Auth0User {
    pub(crate) fn to_row(&self, columns: &[Column]) -> Auth0FdwResult<Row> {
        let mut row = Row::new();
        for tgt_col in columns {
            if tgt_col.name == "created_at" {
                let cell_value = Some(Cell::String(self.created_at.clone()));
                //    None => None, // Or use a default value or handle the error
                // };
                row.push("created_at", cell_value);
            } else if tgt_col.name == "email" {
                row.push("email", Some(Cell::String(self.email.clone())))
            } else if tgt_col.name == "email_verified" {
                row.push("email_verified", Some(Cell::Bool(self.email_verified)))
            } else if tgt_col.name == "identities" {
                let attrs = serde_json::from_str(&self.identities.to_string())?;

                row.push("identities", Some(Cell::Json(JsonB(attrs))))
            }
        }

        Ok(row)
    }
}
