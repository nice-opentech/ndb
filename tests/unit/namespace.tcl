start_server {tags {"namespace"}} {
    test {SET against non exist namespace} {
        catch {r set nonexist:foo bar} err
        format $err
    } {ERR*}

    test {NSDEL against non exist namespace} {
        r nsdel nonexist
        r nsdel nonexist
    }

    test {NSNEW create and delete a new namespace} {
        set a [r nslist]
        r nsnew exist
        set b [r nslist]
        r nsdel exist
        set c [r nslist]
        list $a $b $c
    } {default {default exist} default}

    test {NSNEW create a namespace twice} {
        r nsnew exist
        catch {r nsnew exist} err
        format $err
    } {ERR*}

    test {NSSET set namespace expire} {
        r nsnew ns
        set a [r nsget ns expire]
        r nsset ns expire 30
        set b [r nsget ns expire]
        r nsset ns expire 60
        set c [r nsget ns expire]
        r nsdel ns
        list $a $b $c
    } {0 30 60}

    test {NSSET set namespace maxlen} {
        r nsnew ns
        set a [r nsget ns maxlen]
        r nsset ns maxlen 1024
        set b [r nsget ns maxlen]
        r nsset ns maxlen 2048
        set c [r nsget ns maxlen]
        r nsdel ns
        list $a $b $c
    } {0 1024 2048}

    test {NSSET set namespace pruning} {
        r nsnew ns
        set a [r nsget ns pruning]
        r nsset ns pruning max
        set b [r nsget ns pruning]
        r nsset ns pruning min
        set c [r nsget ns pruning]
        catch {r nsset ns pruning foo} err
        r nsdel ns
        list $a $b $c $err
    } {min max min {ERR*}}

    test {NSSET set invalid config} {
        r nsnew ns
        catch {r nsset ns invalid 123} err
        r nsdel ns
        format $err
    } {ERR*}

    test {NSGET get invalid config} {
        r nsnew ns
        catch {r nsget ns invalid} err
        r nsdel ns
        format $err
    } {ERR*}
}
