use dotenv::dotenv;
use redis::{Client, Commands, Connection};
use std::{collections::BTreeMap, env, num::NonZeroUsize};

fn main() {
    let mut conn = connect();

    basic_set(&mut conn, "alice", "001");
    
    let value1 = basic_get(&mut conn, "alice");
    println!("Value 1: {:?}", value1);

    let hash_key = "member:id";
    let mut id_map = BTreeMap::<String, String>::new();
    id_map.insert(String::from("alice"), String::from("001"));
    id_map.insert(String::from("bob"), String::from("002"));
    hash_set(&mut conn, hash_key, id_map);

    let value2 = hash_get_all(&mut conn, hash_key);
    println!("Value 2: {:?}", value2);

    let list_name = "managers";
    let list_length = list_len(&mut conn, list_name.into());
    println!("List length: {:?}", list_length);

    if list_length > 0 {
        list_pop(&mut conn, list_name.into(), NonZeroUsize::new(list_length));
    }
    
    list_push(&mut conn, list_name, "carol");
    list_push(&mut conn, list_name, "david");

    let list_length = list_len(&mut conn, list_name.into());
    let list_items = list_range(&mut conn, list_name.into(), 0, (list_length - 1) as isize);
    println!("List items: {:?}", list_items);
}

fn connect() -> Connection {
    dotenv().expect(".env file not found");
    let hostname = env::var("REDIS_HOSTNAME").expect("Environment variable REDIS_HOSTNAME not found");
    let password = env::var("REDIS_PASSWORD").expect("Environment variable REDIS_PASSWORD not found");

    let conn_url = format!("redis://:{}@{}", password, hostname);
    Client::open(conn_url)
        .expect("Invalid connection URL")
        .get_connection()
        .expect("Failed to connect to Redis")
}

fn basic_set(conn: &mut Connection, key: &str, value: &str) {
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query::<String>(conn)
        .expect("Failed to execute SET");
}

fn basic_get(conn: &mut Connection, key: &str) -> String {
    redis::cmd("GET")
        .arg(key)
        .query::<String>(conn)
        .expect("Failed to execute GET")
}

fn hash_set(conn: &mut Connection, hash_key: &str, id_map: BTreeMap::<String, String>) {
    redis::cmd("HSET")
        .arg(hash_key)
        .arg(id_map)
        .query::<usize>(conn)
        .expect("Failed to execute HSET");
}

fn hash_get_all(conn: &mut Connection, hash_key: &str) -> BTreeMap::<String, String> {
    redis::cmd("HGETALL")
        .arg(hash_key)
        .query::<BTreeMap::<String, String>>(conn)
        .expect("Failed to execute HGETALL")
}

fn list_push(conn: &mut Connection, list_name: &str, member_name: &str) {
    redis::cmd("LPUSH")
        .arg(list_name)
        .arg(member_name)
        .query::<usize>(conn)
        .expect("Failed to execute LPUSH");
}

fn list_pop(conn: &mut Connection, list_name: String, count: Option<NonZeroUsize>) {
    conn.lpop::<String, Vec<String>>(list_name, count).expect("Failed to execute LPOP");
}

fn list_len(conn: &mut Connection, list_name: String) -> usize {
    conn.llen::<String, usize>(list_name).expect("Failed to execute LLEN")
}

fn list_range(conn: &mut Connection, list_name: String, start: isize, stop: isize) -> Vec<String> {
    conn.lrange::<String, Vec<String>>(list_name, start, stop).expect("Failed to execute LRANGE")
}
