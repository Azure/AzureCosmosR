make_name <- function(n=20)
{
    paste0(sample(letters, n, TRUE), collapse="")
}

write_file <- function(dir, size=1000)
{
    fname <- tempfile(tmpdir=dir)
    bytes <- openssl::rand_bytes(size)
    writeBin(bytes, fname)
    basename(fname)
}
