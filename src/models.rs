use uuid::Uuid;

#[derive(Debug)]
pub struct Person {
    pub id: Uuid,
    pub first_name: String,
    pub last_name: String,
    pub age: u32,
    pub email: String,
}

impl Person {
    pub fn new(first_name: &str, last_name: &str, age: u32, email: &str) -> Self {
        Self {
            id: Uuid::now_v7(),
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
            age,
            email: email.to_string(),
        }
    }
}
