package deltalake

type table struct  {
    name string 
    files []string // underlying table files 
}
