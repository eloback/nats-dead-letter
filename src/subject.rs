use uuid::Uuid;

use crate::error::Result;

pub fn subject_from_str<'a>(subject: &'a str) -> Result<(&'a str, &'a str, Uuid)> {
    let mut parts = subject.split('.').fuse();

    let prefix = parts.next().ok_or_else(|| eyre::eyre!("invalid subject"))?;

    let name = parts.next();
    let id = parts.next();

    if let (Some(name), Some(id)) = (name, id) {
        let parsed_id = Uuid::try_parse(id)?;
        Ok((prefix.into(), name.into(), parsed_id))
    } else {
        Err(eyre::eyre!("invalid subject"))
    }
}
