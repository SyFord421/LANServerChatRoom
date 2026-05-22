use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use std::process;


// Data custom biar nggak kepanjangan nulis di fungsin
type SenderChannel = broadcast::Sender<(String, String)>;
type ReceiverChannel = broadcast::Receiver<(String, String)>;


#[tokio::main]
async fn  main() {
    let listener = tokio::net::TcpListener:: bind("127.0.0.1:8080")
    .await
    .unwrap_or_else(|err|{
        println!("{}", err);
        process::exit(1)
    });
    // Buat Channel Broadcast utama
    // 16 adalah panjang karakter pesan
    let (tx, _) = broadcast::channel::<(String, String)>(16); 
    println!("[Ok] Server Is Running...");
    
    // Tugas main hanyalah menjadi pendengar
    loop {
        if let Ok((socket, _)) = listener.accept().await {
            let tx_clone = tx.clone();
            let rx_clone = tx.subscribe();
            let _ = tokio::spawn(async move{
                handle_users(socket, tx_clone, rx_clone).await;
            });
        }
    }
}


// Fungsi untuk mengurusi User Dari awal Sampe bosen
async fn handle_users(socket: TcpStream, tx: SenderChannel, mut rx: ReceiverChannel) { // nah tipe data custom tadi di gunakan di sini kalau nggak gitu nantinya gini  tx: broadcast::Sender<(String, String)>
    let client_addr = socket.peer_addr(). unwrap().to_string();
    // belah kabel Jadi 2 bagian satu untukmu satu untuku hehe
    let (reader, mut writer) = tokio::io::split(socket);
    // Shadowing biar simple dan biar nggak buta huruf
    let mut reader = BufReader::new(reader);
    // proses perkenalan pake nama asli yah jangan pake nama politik
    let mut name = String::new();
    let _ = writer.write_all(b"Siapa Namamu Tuan?\n").await;//namaku dizz
    // Kirim paksa biar nggak di timbun
    let _ = writer.flush().await;
    if reader.read_line(&mut name).await.is_err() {return;}// kalo gagal baca minta input lagi dengan return ke awal
    let username= name.trim().to_string();
    
    println!("{} Telah Bergabung Dari Port {}", username, client_addr);
    // Umumkan Tamu baru biar ketahuan
    let _ = tx.send(("Server".to_string(), format!("{} Telah Bergabung..", username)));

    let mut msg = String::new();
    loop {
        tokio::select! {
            // dengerin ketikan user 
            res = reader.read_line(&mut msg) => {
                match res {
                    Ok(0) => break,
                    Ok(_) => {
                        let _ = tx.send((username.clone(), msg.trim().to_string()));
                        msg.clear(); 
                    }
                    Err(_) => break,
                }
            }
            result = rx.recv() => {
                if let Ok((sender, txt)) = result {
                    let formated = format_message(&sender, &username, &txt);
                    if writer.write_all(formated.as_bytes()).await.is_err(){break;}
                    let _ = writer.flush().await;
                }
            }
        }
    }
    // Proses Keluar
    println!("[Log] {} Keluar.", username);
    let _ = tx.send(("Server".to_string(), format!("{} meninggalkan obrolan.", username)));
}

fn format_message(sender: &str, username: &str, txt: &str) -> String{
    if sender == username {
        format!("\x1B[1A\x1B[2K[Anda]: {}\n", txt)
    } else if sender == "Server"{
            format!("[INFO]: {}\n", txt)
        } else {
        format!("[{}]: {}\n", sender, txt)
    }
}