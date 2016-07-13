start_server {tags {"info"}} {
    test {ECHO hello world} {
        r echo "hello, world!"
    } {hello, world!}

    test {INFO engine} {
        r info engine
    } {*engine*}

    test {INFO engine namespace} {
        r nsnew foo
        set foo [r info engine foo]
        r nsnew bar
        set bar [r info engine bar]
        list $foo $bar
    } {*engine*foo* *engine*bar*}

    test {INFO backup} {
        r info backup
    } {*backup*OK*}

    test {INFO server} {
        r info server
    } {*server*uptime*address*}

    test {INFO replica} {
        r info replica
    } {*replica*}

    test {INFO command} {
        r info command
    } {*command*ECHO*INFO*}

    test {INFO command cmdname} {
        r info command info
    } {*command*INFO*}

    test {INFO nsstats} {
        r info nsstats
    } {*nsstats*bar*INFO*foo*INFO*}

    test {INFO nsstats nsname} {
        r info nsstats foo
    } {*nsstats*foo*INFO*}

    test {SLOWLOG} {
        set len [r slowlog len]
        set val [r slowlog get]
        list $len $val
    } {0 {}}
}
