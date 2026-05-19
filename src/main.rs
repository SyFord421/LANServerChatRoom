use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::process;

struct Client {
    username: String,// nama user
    address: String,// alamat port
    writer: WriteHalf<TcpStream>// Writer milik user
}

// 1. Untuk mengurus 1 User di awal
// Fungsi untuk handle Pesan User
async fn handle_request(socket: TcpStream, clients: Arc<Mutex<Vec<Client>>>) {

// Ambil alamat IP & Port unik si user sebagai KTP
    let client_addr = socket.peer_addr().unwrap().to_string(); 
    
// belah socket jadi 2 bagian
    let(reader, mut writer) = tokio::io::split(socket);
// Shadowing biar bisa baca input user
    let mut reader = BufReader::new(reader);
    
// minta input username
    let mut name = String::new();

    let _ = writer.write_all(b"Siapa Namamu Tuan?: ").await;
    
// flush biar nggak nyangkut di buffer
    let _ = writer.flush().await;
    let _ = reader.read_line(&mut name).await;
    let username = name.trim().to_string();
    println!("[Log] {} Telah bergabung dari {}", username, client_addr);
    
    {// delimiter agar kunci bisa di buka
    
// kunci daftar agar tidak ada rebutan (.await menunggu giliran input user)
        let mut list = clients.lock().await;
        let new_client = Client {
            username: username,
            address: client_addr.clone(),
            writer: writer
        };
        list.push(new_client);
    }// <- Kunci Otomatis akan di buka di sini
    
    loop {
    let mut msg = String::new();
    match reader.read_line(&mut msg).await {
        Ok(0) => {
            break;
        },
        Ok(_) => {
            // kunci semuanya sementara karena akan melakukan siaran 
            let mut list = clients.lock().await;
            // lakukan perulangan untuk mengirimkan pesan ke semua orang 
            for client in list.iter_mut() {
                let broadcast = format!("\x1B[1A\x1B[2K[{}]: {}\n", client.username, msg.trim());
                let _ = client.writer.write_all(broadcast.as_bytes()).await;
                let _ = client.writer.flush().await;
                println!("[Log]| {}", broadcast);
            }
        },
        Err(_) => break,
    }
} 
{
// Fungsi untuk menghapus User saat keluar
    let mut list = clients.lock().await;
    list.retain(|client| client.address != client_addr);
}// Buka Lock di sini
}

#[tokio::main]
async fn main() {
// Buat daftar buku bersama (Arc) dan 1 alat tulis untuk di oper bergantian (Mutex)
    let clients: Arc<Mutex<Vec<Client>>> = Arc::new(Mutex::new(Vec::new()));
// Siapkan Jalur Komunikasi (Port)
    let listener = tokio::net:: TcpListener::bind("127.0.0.1:8080").await.unwrap_or_else( |err| {
        println!("{}", err);
        process::exit(1)
    });

    println!("[OK] Server Is Running...");
    
    loop {
        if let Ok((socket, _ )) = listener.accept().await {
        let client_clone = Arc::clone(&clients);
        tokio::spawn(async move {
            handle_request(socket, client_clone).await;
        });
        }else{
            println!("[Warning] Gagal menerima tamu baru, mencoba lagi...");
        }
    }
}