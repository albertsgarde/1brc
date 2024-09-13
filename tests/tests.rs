const TEST_DIR: &str = "tests/test_files";

macro_rules! brc_tests {
    ($($name:ident: $file_name:expr,)*) => {
    $(
        #[test]
        fn $name() {
            let test_dir_path = std::path::Path::new(TEST_DIR);

            let file_path = test_dir_path.join($file_name);

            let data_file_path = file_path.with_extension("txt");
            let out_file_path = file_path.with_extension("out");
            let expected = std::fs::read_to_string(out_file_path).unwrap();
            let result = brc::v1::summarize(&data_file_path, None, 4);
            match result {
                Ok(summary) => assert_eq!(expected, summary),
                Err(_) => panic!("Error summarizing file {:?}", data_file_path.file_name().unwrap()),
            }
        }
    )*
    }
}

brc_tests! {
    m1l: "measurements-1",
    m2l: "measurements-2",
    m6l: "measurements-3",
    m10l: "measurements-10",
    m20l: "measurements-20",
    m10000_unique_keys: "measurements-10000-unique-keys",
    boundaries: "measurements-boundaries",
    complex_utf8: "measurements-complex-utf8",
    dot: "measurements-dot",
    rounding: "measurements-rounding",
    short: "measurements-short",
    shortest: "measurements-shortest",
}
