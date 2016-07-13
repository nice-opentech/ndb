start_server {tags {"oset"}} {
    proc create_oset {key members} {
        r del $key
        foreach {member} $members {
            r oadd $key $member
        }
    }

    proc basics {} {
        test "OSET basic OADD" {
            r del otmp
            r oadd otmp 10
            r oadd otmp 20
            r oadd otmp 30
            assert_equal {10 20 30} [r orange otmp 0 -1]
        }

        test "OSET element can't be set to NaN with OADD" {
            assert_error "ERR*" {r oadd myoset nan}
        }

        test "OSET element can't be set to NaN with ZINCRBY" {
            assert_error "ERR*" {r oadd myoset incrby nan}
        }

        test "OADD with options syntax error with incomplete pair" {
            r del otmp
            catch {r oadd otmp xx} err
            set err
        } {ERR*}

        test "OADD returns the number of elements actually added" {
            r del otmp
            r oadd otmp 10
            set retval [r oadd otmp 10 20 30]
            assert {$retval == 2}
        }

        # test "ZADD INCR works like ZINCRBY" {
        #     r del ztmp
        #     r zadd ztmp 10 x 20 y 30 z
        #     r zadd ztmp INCR 15 x
        #     assert {[r zscore ztmp x] == 25}
        # }

        # test "ZADD INCR works with a single score-elemenet pair" {
        #     r del ztmp
        #     r zadd ztmp 10 x 20 y 30 z
        #     catch {r zadd ztmp INCR 15 x 10 y} err
        #     set err
        # } {ERR*}

        # test "ZINCRBY calls leading to NaN result in error" {
        #     r zincrby myzset +inf abc
        #     assert_error "*NaN*" {r zincrby myzset -inf abc}
        # }

        # test {ZINCRBY does not work variadic even if shares ZADD implementation} {
        #     r del myzset
        #     catch {r zincrby myzset 10 a 20 b 30 c} e
        #     assert_match {*ERR*wrong*number*arg*} $e
        # }

        test "OCARD basics" {
            r del otmp
            r oadd otmp 10 20 30
            assert_equal 3 [r ocard otmp]
            assert_equal 0 [r ocard odoesntexist]
        }

        test "OREM removes key after last element is removed" {
            r del otmp
            r oadd otmp 10
            r oadd otmp 20

            assert_equal 1 [r exists otmp]
            assert_equal 0 [r orem otmp 30]
            assert_equal 1 [r orem otmp 20]
            assert_equal 1 [r orem otmp 10]
            assert_equal 0 [r exists otmp]
        }

        test "OREM variadic version" {
            r del otmp
            r oadd otmp 10 20 30
            assert_equal 2 [r orem otmp 0 10 30 40]
            assert_equal 0 [r orem otmp 0 40]
            assert_equal 1 [r orem otmp 20]
            r exists otmp
        } {0}

        test "OREM variadic version -- remove elements after key deletion" {
            r del otmp
            r oadd otmp 10 20 30
            r orem otmp 0 10 20 30 40 50
        } {3}

        test "ORANGE basics" {
            r del otmp
            r oadd otmp 1
            r oadd otmp 2
            r oadd otmp 3
            r oadd otmp 4

            assert_equal {1 2 3 4} [r orange otmp 0 -1]
            assert_equal {1 2 3} [r orange otmp 0 -2]
            assert_equal {2 3 4} [r orange otmp 1 -1]
            assert_equal {2 3} [r orange otmp 1 -2]
            assert_equal {3 4} [r orange otmp -2 -1]
            assert_equal {3} [r orange otmp -2 -2]

            # out of range start index
            assert_equal {1 2 3} [r orange otmp -5 2]
            assert_equal {1 2} [r orange otmp -5 1]
            assert_equal {} [r orange otmp 5 -1]
            assert_equal {} [r orange otmp 5 -2]

            # out of range end index
            assert_equal {1 2 3 4} [r orange otmp 0 5]
            assert_equal {2 3 4} [r orange otmp 1 5]
            assert_equal {} [r orange otmp 0 -5]
            assert_equal {} [r orange otmp 1 -5]
        }

        test "OREVRANGE basics" {
            r del otmp
            r oadd otmp 1
            r oadd otmp 2
            r oadd otmp 3
            r oadd otmp 4

            assert_equal {4 3 2 1} [r orevrange otmp 0 -1]
            assert_equal {4 3 2} [r orevrange otmp 0 -2]
            assert_equal {3 2 1} [r orevrange otmp 1 -1]
            assert_equal {3 2} [r orevrange otmp 1 -2]
            assert_equal {2 1} [r orevrange otmp -2 -1]
            assert_equal {2} [r orevrange otmp -2 -2]

            # out of range start index
            assert_equal {4 3 2} [r orevrange otmp -5 2]
            assert_equal {4 3} [r orevrange otmp -5 1]
            assert_equal {} [r orevrange otmp 5 -1]
            assert_equal {} [r orevrange otmp 5 -2]

            # out of range end index
            assert_equal {4 3 2 1} [r orevrange otmp 0 5]
            assert_equal {3 2 1} [r orevrange otmp 1 5]
            assert_equal {} [r orevrange otmp 0 -5]
            assert_equal {} [r orevrange otmp 1 -5]
        }

        # test "ZRANK/ZREVRANK basics - $encoding" {
        #     r del zranktmp
        #     r oadd zranktmp 10 x
        #     r oadd zranktmp 20 y
        #     r oadd zranktmp 30 z
        #     assert_equal 0 [r zrank zranktmp x]
        #     assert_equal 1 [r zrank zranktmp y]
        #     assert_equal 2 [r zrank zranktmp z]
        #     assert_equal "" [r zrank zranktmp foo]
        #     assert_equal 2 [r zrevrank zranktmp x]
        #     assert_equal 1 [r zrevrank zranktmp y]
        #     assert_equal 0 [r zrevrank zranktmp z]
        #     assert_equal "" [r zrevrank zranktmp foo]
        # }

        # test "ZRANK - after deletion - $encoding" {
        #     r zrem zranktmp y
        #     assert_equal 0 [r zrank zranktmp x]
        #     assert_equal 1 [r zrank zranktmp z]
        # }

        # test "ZINCRBY - can create a new sorted set - $encoding" {
        #     r del zset
        #     r zincrby zset 1 foo
        #     assert_equal {foo} [r orange zset 0 -1]
        #     assert_equal 1 [r zscore zset foo]
        # }

        # test "ZINCRBY - increment and decrement - $encoding" {
        #     r zincrby zset 2 foo
        #     r zincrby zset 1 bar
        #     assert_equal {bar foo} [r orange zset 0 -1]

        #     r zincrby zset 10 bar
        #     r zincrby zset -5 foo
        #     r zincrby zset -5 bar
        #     assert_equal {foo bar} [r orange zset 0 -1]

        #     assert_equal -2 [r zscore zset foo]
        #     assert_equal  6 [r zscore zset bar]
        # }

        proc create_default_oset {} {
            create_oset oset {-99 1 2 3 4 5 99}
        }

        test "ORANGEBYSCORE/OREVRANGEBYSCORE basics" {
            create_default_oset

            # inclusive range
            assert_equal {-99 1 2} [r orangebymember oset -inf 2]
            assert_equal {1 2 3}   [r orangebymember oset 0 3]
            assert_equal {3 4 5}   [r orangebymember oset 3 6]
            assert_equal {4 5 99}  [r orangebymember oset 4 +inf]
            assert_equal {2 1 -99} [r orevrangebymember oset 2 -inf]
            assert_equal {3 2 1}   [r orevrangebymember oset 3 0]
            assert_equal {5 4 3}   [r orevrangebymember oset 6 3]
            assert_equal {99 5 4}  [r orevrangebymember oset +inf 4]
            # assert_equal 3 [r zcount oset 0 3]

            # exclusive range
            assert_equal {-99 1} [r orangebymember oset (-inf (2]
            assert_equal {1 2}   [r orangebymember oset (0 (3]
            assert_equal {4 5}   [r orangebymember oset (3 (6]
            assert_equal {5 99}  [r orangebymember oset (4 (+inf]
            assert_equal {1 -99} [r orevrangebymember oset (2 (-inf]
            assert_equal {2 1}   [r orevrangebymember oset (3 (0]
            assert_equal {5 4}   [r orevrangebymember oset (6 (3]
            assert_equal {99 5}  [r orevrangebymember oset (+inf (4]
            # assert_equal 2 [r zcount oset (0 (3]

            # test empty ranges
            r orem oset -99
            r orem oset 99

            # inclusive
            assert_equal {} [r orangebymember oset 4 2]
            assert_equal {} [r orangebymember oset 6 +inf]
            assert_equal {} [r orangebymember oset -inf -6]
            assert_equal {} [r orevrangebymember oset +inf 6]
            assert_equal {} [r orevrangebymember oset -6 -inf]

            # exclusive
            assert_equal {} [r orangebymember oset (4 (2]
            assert_equal {} [r orangebymember oset 2 (2]
            assert_equal {} [r orangebymember oset (2 2]
            assert_equal {} [r orangebymember oset (6 (+inf]
            assert_equal {} [r orangebymember oset (-inf (-6]
            assert_equal {} [r orevrangebymember oset (+inf (6]
            assert_equal {} [r orevrangebymember oset (-6 (-inf]

            # empty inner range
            # assert_equal {} [r orangebymember oset 2.4 2.6]
            # assert_equal {} [r orangebymember oset (2.4 2.6]
            # assert_equal {} [r orangebymember oset 2.4 (2.6]
            # assert_equal {} [r orangebymember oset (2.4 (2.6]
        }

        test "ORANGEBYMEMBER with LIMIT" {
            create_default_oset
            assert_equal {1 2}   [r orangebymember oset 0 10 LIMIT 0 2]
            assert_equal {3 4 5} [r orangebymember oset 0 10 LIMIT 2 3]
            assert_equal {3 4 5} [r orangebymember oset 0 10 LIMIT 2 10]
            assert_equal {}      [r orangebymember oset 0 10 LIMIT 20 10]
            assert_equal {5 4}   [r orevrangebymember oset 10 0 LIMIT 0 2]
            assert_equal {3 2 1} [r orevrangebymember oset 10 0 LIMIT 2 3]
            assert_equal {3 2 1} [r orevrangebymember oset 10 0 LIMIT 2 10]
            assert_equal {}      [r orevrangebymember oset 10 0 LIMIT 20 10]
        }

        test "OREMRANGEBYRANK basics" {
            proc remrangebyrank {min max} {
                create_oset oset {1 2 3 4 5}
                assert_equal 1 [r exists oset]
                r oremrangebyrank oset $min $max
            }

            # inner range
            assert_equal 3 [remrangebyrank 1 3]
            assert_equal {1 5} [r orange oset 0 -1]

            # start underflow
            assert_equal 1 [remrangebyrank -10 0]
            assert_equal {2 3 4 5} [r orange oset 0 -1]

            # start overflow
            assert_equal 0 [remrangebyrank 10 -1]
            assert_equal {1 2 3 4 5} [r orange oset 0 -1]

            # end underflow
            assert_equal 0 [remrangebyrank 0 -10]
            assert_equal {1 2 3 4 5} [r orange oset 0 -1]

            # end overflow
            assert_equal 5 [remrangebyrank 0 10]
            assert_equal {} [r orange oset 0 -1]

            # destroy when empty
            assert_equal 5 [remrangebyrank 0 4]
            assert_equal 0 [r exists oset]
        }

        test "ORANGEBYMEMBER with non-value start or stop" {
            assert_error "ERR*" {r oremrangebyrank fooz str 1}
            assert_error "ERR*" {r oremrangebyrank fooz 1 str}
            assert_error "ERR*" {r oremrangebyrank fooz 1 NaN}
        }

    #     test "ZUNIONSTORE against non-existing key doesn't set destination - $encoding" {
    #         r del oseta
    #         assert_equal 0 [r zunionstore dst_key 1 oseta]
    #         assert_equal 0 [r exists dst_key]
    #     }

    #     test "ZUNIONSTORE with empty set - $encoding" {
    #         r del oseta osetb
    #         r oadd oseta 1 a
    #         r oadd oseta 2 b
    #         r zunionstore osetc 2 oseta osetb
    #         r orange osetc 0 -1 withscores
    #     } {a 1 b 2}

    #     test "ZUNIONSTORE basics - $encoding" {
    #         r del oseta osetb osetc
    #         r oadd oseta 1 a
    #         r oadd oseta 2 b
    #         r oadd oseta 3 c
    #         r oadd osetb 1 b
    #         r oadd osetb 2 c
    #         r oadd osetb 3 d

    #         assert_equal 4 [r zunionstore osetc 2 oseta osetb]
    #         assert_equal {a 1 b 3 d 3 c 5} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZUNIONSTORE with weights - $encoding" {
    #         assert_equal 4 [r zunionstore osetc 2 oseta osetb weights 2 3]
    #         assert_equal {a 2 b 7 d 9 c 12} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZUNIONSTORE with a regular set and weights - $encoding" {
    #         r del seta
    #         r sadd seta a
    #         r sadd seta b
    #         r sadd seta c

    #         assert_equal 4 [r zunionstore osetc 2 seta osetb weights 2 3]
    #         assert_equal {a 2 b 5 c 8 d 9} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZUNIONSTORE with AGGREGATE MIN - $encoding" {
    #         assert_equal 4 [r zunionstore osetc 2 oseta osetb aggregate min]
    #         assert_equal {a 1 b 1 c 2 d 3} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZUNIONSTORE with AGGREGATE MAX - $encoding" {
    #         assert_equal 4 [r zunionstore osetc 2 oseta osetb aggregate max]
    #         assert_equal {a 1 b 2 c 3 d 3} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZINTERSTORE basics - $encoding" {
    #         assert_equal 2 [r zinterstore osetc 2 oseta osetb]
    #         assert_equal {b 3 c 5} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZINTERSTORE with weights - $encoding" {
    #         assert_equal 2 [r zinterstore osetc 2 oseta osetb weights 2 3]
    #         assert_equal {b 7 c 12} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZINTERSTORE with a regular set and weights - $encoding" {
    #         r del seta
    #         r sadd seta a
    #         r sadd seta b
    #         r sadd seta c
    #         assert_equal 2 [r zinterstore osetc 2 seta osetb weights 2 3]
    #         assert_equal {b 5 c 8} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZINTERSTORE with AGGREGATE MIN - $encoding" {
    #         assert_equal 2 [r zinterstore osetc 2 oseta osetb aggregate min]
    #         assert_equal {b 1 c 2} [r orange osetc 0 -1 withscores]
    #     }

    #     test "ZINTERSTORE with AGGREGATE MAX - $encoding" {
    #         assert_equal 2 [r zinterstore osetc 2 oseta osetb aggregate max]
    #         assert_equal {b 2 c 3} [r orange osetc 0 -1 withscores]
    #     }

    #     foreach cmd {ZUNIONSTORE ZINTERSTORE} {
    #         test "$cmd with +inf/-inf scores - $encoding" {
    #             r del osetinf1 osetinf2

    #             r oadd osetinf1 +inf key
    #             r oadd osetinf2 +inf key
    #             r $cmd osetinf3 2 osetinf1 osetinf2
    #             assert_equal inf [r zscore osetinf3 key]

    #             r oadd osetinf1 -inf key
    #             r oadd osetinf2 +inf key
    #             r $cmd osetinf3 2 osetinf1 osetinf2
    #             assert_equal 0 [r zscore osetinf3 key]

    #             r oadd osetinf1 +inf key
    #             r oadd osetinf2 -inf key
    #             r $cmd osetinf3 2 osetinf1 osetinf2
    #             assert_equal 0 [r zscore osetinf3 key]

    #             r oadd osetinf1 -inf key
    #             r oadd osetinf2 -inf key
    #             r $cmd osetinf3 2 osetinf1 osetinf2
    #             assert_equal -inf [r zscore osetinf3 key]
    #         }

    #         test "$cmd with NaN weights $encoding" {
    #             r del osetinf1 osetinf2

    #             r oadd osetinf1 1.0 key
    #             r oadd osetinf2 1.0 key
    #             assert_error "*weight*not*float*" {
    #                 r $cmd osetinf3 2 osetinf1 osetinf2 weights nan nan
    #             }
    #         }
    #     }
    # }

    basics

    # test {ZINTERSTORE regression with two sets, intset+hashtable} {
    #     r del seta setb setc
    #     r sadd set1 a
    #     r sadd set2 10
    #     r zinterstore set3 2 set1 set2
    # } {0}

    # test {ZUNIONSTORE regression, should not create NaN in scores} {
    #     r oadd z -inf neginf
    #     r zunionstore out 1 z weights 0
    #     r orange out 0 -1 withscores
    # } {neginf 0}

    # test {ZINTERSTORE #516 regression, mixed sets and ziplist osets} {
    #     r sadd one 100 101 102 103
    #     r sadd two 100 200 201 202
    #     r oadd three 1 500 1 501 1 502 1 503 1 100
    #     r zinterstore to_here 3 one two three WEIGHTS 0 0 1
    #     r orange to_here 0 -1
    # } {100}

    # test {ZUNIONSTORE result is sorted} {
    #     # Create two sets with common and not common elements, perform
    #     # the UNION, check that elements are still sorted.
    #     r del one two dest
    #     set cmd1 [list r oadd one]
    #     set cmd2 [list r oadd two]
    #     for {set j 0} {$j < 1000} {incr j} {
    #         lappend cmd1 [expr rand()] [randomInt 1000]
    #         lappend cmd2 [expr rand()] [randomInt 1000]
    #     }
    #     {*}$cmd1
    #     {*}$cmd2
    #     assert {[r zcard one] > 100}
    #     assert {[r zcard two] > 100}
    #     r zunionstore dest 2 one two
    #     set oldscore 0
    #     foreach {ele score} [r orange dest 0 -1 withscores] {
    #         assert {$score >= $oldscore}
    #         set oldscore $score
    #     }
    # }

    proc stressers {} {
        set elements 128

        test "OSET sorting stresser" {
            set delta 0
            for {set test 0} {$test < 2} {incr test} {
                unset -nocomplain auxarray
                array set auxarray {}
                set auxlist {}
                r del myoset
                for {set i 0} {$i < $elements} {incr i} {
                    if {$test == 0} {
                        set member [expr int(rand())]
                    } else {
                        set member [expr int(rand()*10)]
                    }
                    set auxarray($i) $member
                    r oadd myoset $member
                    # Random update
                    if {[expr rand()] < .2} {
                        set j [expr int(rand()*1000)]
                        if {$test == 0} {
                            set member [expr int(rand())]
                        } else {
                            set member [expr int(rand()*10)]
                        }
                        set auxarray($j) $member
                        r oadd myoset $member
                    }
                }
                foreach {index member} [array get auxarray] {
                    lappend auxlist $member
                }
                set auxlist [lsort -unique $auxlist]

                set fromredis [r orange myoset 0 -1]
                set delta 0
                for {set i 0} {$i < [llength $fromredis]} {incr i} {
                    if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                        incr delta
                    }
                }
            }
            assert_equal 0 $delta
        }

        # test "OSETs ZRANK augmented skip list stress testing - $encoding" {
        #     set err {}
        #     r del myoset
        #     for {set k 0} {$k < 2000} {incr k} {
        #         set i [expr {$k % $elements}]
        #         if {[expr int(rand())] < .2} {
        #             r orem myoset $i
        #         } else {
        #             set score [expr int(rand())]
        #             r oadd myoset $score $i
        #             assert_encoding $encoding myoset
        #         }

        #         set card [r zcard myoset]
        #         if {$card > 0} {
        #             set index [randomInt $card]
        #             set ele [lindex [r orange myoset $index $index] 0]
        #             set rank [r zrank myoset $ele]
        #             if {$rank != $index} {
        #                 set err "$ele RANK is wrong! ($rank != $index)"
        #                 break
        #             }
        #         }
        #     }
        #     assert_equal {} $err
        # }
    }

    tags {"slow"} {
        stressers
    }

    proc maxlen {} {
        test "OADD maxlen 5 pruning default (min)" {
            r del oset
            r oadd oset maxlen 5 1 2 3
            set a [r orange oset 0 -1]
            r oadd oset 4 5 6
            set b [r orevrange oset 0 -1]
            list $a $b
        } {{1 2 3} {6 5 4 3 2}}

        test "OADD maxlen 5 pruning max" {
            r del oset
            r oadd oset maxlen 5 pruning max 1 2 3
            set a [r orange oset 0 -1]
            r oadd oset 4 5 6
            set b [r orevrange oset 0 -1]
            list $a $b
        } {{1 2 3} {5 4 3 2 1}}
    }

    tags {"maxlen"} {
        maxlen
    }
}
