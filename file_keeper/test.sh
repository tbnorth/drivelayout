log () {
    echo -e '\n'"$@"'\n'
    echo -e "$@" >> _test.log
}
run () {
    PYTHONPATH=.. python3 file_db.py --db-file _test.db $@
    sha256sum _test.db >>_test.log 2>&1
}    
run_all () {
    for i in _test.db _test.log; do if [ -e $i ]; then rm $i; fi; done
    log "Expecting --dry-run error"
    run --path . --dry-run
    log "Run creating DB"
    run --path .
    log "--dry-run test"
    run --path .
    log "regular run test"
    run --path .
    log "list files"
    run --list-files
    log "list dupes"
    run --list-dupes
    log
    cat _test.log
    rm _test.db _test.log
}

run_all 2>&1 | less

