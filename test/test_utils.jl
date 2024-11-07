using HPCMod
using Random
using DataFrames
using Dates

@testset "duration_format test" begin
    test_list = (
        # output, days hours minutes seconds millisecond
        ("1-01:01:11.012", 1, 1, 1, 11, 12),
        ("0-00:00:00.012", 0, 0, 0, 0, 12),
        ("0-00:00:01.012", 0, 0, 0, 1, 12),
        ("0-00:00:15.012", 0, 0, 0, 15, 12),
        ("0-00:01:00.012", 0, 0, 1, 0, 12),
        ("0-00:01:02.999", 0, 0, 1, 2, 999),
        ("0-00:15:15.000", 0, 0, 15, 15, 0),
        ("0-01:00:00.000", 0, 1, 0, 0, 0),
        ("0-11:00:00.000", 0, 11, 0, 0, 0),
        ("123-23:01:09.200", 123, 23, 1, 9, 200),
    )
    for (comp, days, hours, minutes, seconds, millisecond) in test_list
        @test duration_format(Millisecond(days*24*3600000 + hours*3600000 + minutes*60000 + seconds*1000 + millisecond)) == comp
    end
end