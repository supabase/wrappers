use syn::{
    parse::{Parse, ParseStream, Result},
    Attribute, Error, Ident, Meta, NestedMeta,
};

// Parse a FDW type with optional feature config attributes
//
// #[cfg(feature = "your-feature")]
// MyFdwType
//
#[derive(Clone)]
pub(crate) struct FdwType {
    pub attr: Option<Attribute>,
    pub fdw: Ident,
}

impl Parse for FdwType {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let attr = if attrs.is_empty() {
            None
        } else if attrs.len() == 1 {
            let attr = attrs.first().unwrap();
            let meta = attr.parse_meta()?;
            let err = Error::new_spanned(attr, "expect #[cfg(feature = \"...\")] attribute");
            match meta {
                Meta::List(meta_list) => {
                    let segments = &meta_list.path.segments;
                    if segments.len() != 1 || segments.first().unwrap().ident != "cfg" {
                        return Err(err);
                    }
                    let nested = &meta_list.nested;
                    if nested.len() != 1 {
                        return Err(err);
                    }
                    match nested.first().unwrap() {
                        NestedMeta::Meta(meta) => match meta {
                            Meta::NameValue(nv) => {
                                let segments = &nv.path.segments;
                                if segments.len() != 1
                                    || segments.first().unwrap().ident != "feature"
                                {
                                    return Err(err);
                                }
                            }
                            _ => {
                                return Err(err);
                            }
                        },
                        _ => {
                            return Err(err);
                        }
                    }
                }
                _ => {
                    return Err(err);
                }
            }
            Some(attr.clone())
        } else {
            let last_attr = attrs.last().unwrap().clone();
            let err = Error::new_spanned(last_attr, "only one feature config allowd");
            return Err(err);
        };
        let fdw = input.parse()?;
        Ok(Self { attr, fdw })
    }
}
