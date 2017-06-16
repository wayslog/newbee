extern crate libnewbee;

fn main() {
    use std::fs::File;
    let mut file = File::open("rdb/dump.rdb").unwrap();
    let mut dparser = libnewbee::DefaultRdbParser::default();
    let parsed = dparser.read_to_cmd(&mut file).unwrap();
    for cmdline in parsed {
        let libnewbee::RedisCmd(cmds) = cmdline;
        let rcmd = cmds[0].clone();
        let key = cmds[1].clone();
        print!("cmd: {} {}",
               String::from_utf8_lossy(&rcmd.into_data()),
               String::from_utf8_lossy(&key.into_data()));
        for data in cmds.into_iter().skip(2) {
            print!(" {}", String::from_utf8_lossy(&data.into_data()));
        }
        println!("");
    }
}
