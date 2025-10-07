use uuid::Uuid;

use crate::error::Result;

pub fn subject_from_str<'a>(expected_prefix: &'a str, subject: &'a str) -> Result<(&'a str, Uuid)> {
    let mut parts = subject.split('.').fuse();

    let prefix = parts.next().ok_or_else(|| eyre::eyre!("invalid subject"))?;
    if prefix != expected_prefix {
        return Err(eyre::eyre!("invalid prefix"));
    }

    let name = parts.next();
    let id = parts.next();

    if let (Some(name), Some(id)) = (name, id) {
        let parsed_id = Uuid::try_parse(id)?;
        Ok((name.into(), parsed_id))
    } else {
        Err(eyre::eyre!("invalid subject"))
    }
}
