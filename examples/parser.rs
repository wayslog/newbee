extern crate libnewbee;

fn main() {
    use std::fs::File;
    let mut file = File::open("./rdb/dump.rdb").unwrap();
    let mut dparser = libnewbee::DefaultRdbParser::default();
    let parsed = dparser.read_to_cmd(&mut file).unwrap();
    for cmdline in parsed {
        let datas = cmdline.into_data();
        for data in datas.into_iter() {
            print!("{} ", String::from_utf8_lossy(&data));
        }
        println!("");
    }
}
