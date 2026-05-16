use std::net::{TcpListener, TcpStream};// untuk TCP listener untuk mendengarkan input streamer untuk mengirimkan pesan
use std::io::{BufRead, BufReader, Write};// Bufread/BufReaderuntuk membaca input Write untuk menuliskan pesan
use std::{process, thread};//Process process gunanya untuk mengelola thread luar yang di sebut child dan Thread untuk melakukan paralism
use std::sync::{Mutex, Arc};

fn client_handler(mut stream: TcpStream, clients: Arc<Mutex<Vec<TcpStream>>>) {
    let stream_cloned = stream.try_clone().expect("[!] Gagal melakukan Kloning pada stream");// bikin 2 cabang buat di baca dan di tulis
    let mut reader = BufReader::new(stream_cloned);// Kasih clone buat dia baca gunanya ini adalah untuk membaca data yang ada di buffer 
    //minta input nama
    let mut name = String::new();
    let _ = stream.write_all(b"Hallo Siapa namamu tuan: ");
    let _ = stream.flush(); //karena biasanya nyangkut di buffer kita paksa keluar
    if reader.read_line(&mut name).is_err(){return;}//Tolong ambil satu baris pesan dari ember (buffer), terus tuangin ke variabel name.
    let username = name.trim().to_string();// memotong karakter \n mengubah byte jadi String
    println!("[+1] {} Telah Bergabung", username);
    // setelah username di tambahkan mulai loop untuk chat
    loop {
        let mut msg = String::new();
        match reader.read_line(&mut msg) {
            Ok(0) => {
            println!("[-1] {} Telah Keluar", username);
            break;
            },
            Ok(_) => {
                let user_msg = format!("[{}]: {}\n", username, msg.trim());
                let mut list = clients.lock().unwrap();
                for client in list.iter_mut(){
                    let _ = client.write_all(user_msg.as_bytes());
                    let _ = client.flush();
                }
            }
            Err(_) => break,
        }
    }
}


fn main() {
    // Definisikan List untuk menyimpan pesan Dari Client agar bisa di bagikan
    let clients = Arc::new(Mutex::new(Vec::new()));
    // Koneksi Jalur dan port
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap_or_else(|err|{
        eprintln!("Error: {}", err);
        process::exit(1);
    });
    println!("[!] Server is running");
    for stream in listener.incoming() {
        match stream {
        Ok(s) => {
            let clients_clone = Arc::clone(&clients);
            {
            let mut list = clients.lock().unwrap();
                list.push(s.try_clone().expect("Gagal melakukan clone"));
            }
            thread::spawn(move || {
                client_handler(s, clients_clone)
            });
            }
            Err(e) => eprintln!("{}", e),
        }
    }
}