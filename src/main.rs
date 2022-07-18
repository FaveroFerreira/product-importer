use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let file = File::open("./atelie-catalogo-produtos(1).csv").expect("failed to open file");

    let buffer = BufReader::new(file);

    let books = buffer
        .lines()
        .skip(6)
        .map(|result| result.unwrap())
        .map(Book::from_line)
        .collect::<Vec<Book>>();

    let db_pool = get_db_pool().await;

    for book in books {
        persist_book(&db_pool, book).await;
    }
}

async fn get_db_pool() -> PgPool {
    PgPoolOptions::new()
        .connect("postgres://root:root@localhost:5432/products")
        .await
        .expect("failed to build pg pool")
}

async fn persist_book(pool: &PgPool, book: Book) {
    sqlx::query("INSERT INTO book(id, title, isbn, author, description, price) VALUES($1, $2, $3, $4, $5, $6)")
        .bind(book.id)
        .bind(book.title)
        .bind(book.isbn)
        .bind(book.author)
        .bind(book.description)
        .bind(book.price)
        .execute(pool)
        .await
        .unwrap();
}

#[derive(Debug)]
struct Book {
    id: Uuid,
    title: String,
    isbn: String,
    author: String,
    description: String,
    price: String,
}

impl Book {
    pub fn from_line(line: String) -> Book {
        let split = line.split(';').collect::<Vec<_>>();

        Book {
            id: Uuid::new_v4(),
            title: split.get(0).unwrap().to_string(),
            isbn: split.get(1).unwrap().to_string(),
            author: split.get(2).unwrap().to_string(),
            description: split.get(3).unwrap().to_string(),
            price: split.get(4).unwrap().to_string(),
        }
    }
}
