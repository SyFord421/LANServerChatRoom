use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::sync::Arc;

/// 1. Untuk mengurus 1 User di awal
// Fungsi untuk handle Pesan User
async fn handle_request(socket: TcpStream, clients: Arc<Mutex<Vec<WriteHalf<TcpStream>>>>) {
// belajar Socket jadi 2 bagian
    let(reader, writer) = tokio::io::split(socket);
// Shadowing biar bisa baca input user
    let mut reader = BufReader::new(reader);
    
// minta input username!("")
    let mut name = String::new();
// ubah jadi mutabel bisa biasa di pake buat nulis
    let mut mut_writer = writer;
    let _ = mut_writer.write_all(b"Siapa Namamu Tuan?: ").await;
    
// flush biar nggak nyangkut di buffer
    let _ = mut_writer.flush().await;
    let _ = reader.read_line(&mut name).await;
    let username = name.trim().to_string();
    println!("[Log] {} Telah bergabung", username);
    
    
    let current_writer_index;
    {
// kunci daftar agar tidak ada rebutan (.await menunggu giliran input user)
        let mut list = clients.lock().await;
// Masukkan bagian "menulis" ke dalam daftar global(semua user)
        list.push(mut_writer);
        current_writer_index = list.len() - 1; // catat nomor antrean
        
    }// <- Kunci Otomatis akan di buka di sini
    loop {
    let mut msg = String::new();
    match reader.read_line(&mut msg).await {
        Ok(0) => {
            break;
        },
        Ok(_) => {
            let broadcast = format!("\x1B[1A\x1B[2K[{}]: {}\n", username, msg.trim());
            // kunci semuanya sementara karena akan melakukan siaran 
            let mut list = clients.lock().await;
            // lakukan perulangan untuk mengirimkan pesan ke semua orang 
            for client in list.iter_mut() {
                let _ = client.write_all(broadcast.as_bytes()).await;
                let _ = client.flush().await;
            }
        },
        Err(_) => break,
    }
}
}

#[tokio::main]
async fn main() {
// Buat daftar buku bersama (Arc) dan 1 alat tulis untuk di oper bergantian (Mutex)
    let clients = Arc::new(Mutex::new(Vec::new()));
// Siapkan Jalur Komunikasi (Port)
    let listener = tokio::net:: TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("[OK] Server Is Running...");
    
    loop {
        let (socket, _ )  = listener.accept().await.unwrap();
        let client_clone = Arc::clone(&clients);
        tokio::spawn(async move {
            handle_request(socket, client_clone).await;
        });
    }
}