function compute()
{
    $.ajax({
        type: "POST",
        contentType: "application/json; charset=utf-8",
        url: "/mapreduce/Home/Compute/",
        data: JSON.stringify({
            gpu: false
        }),
        datatype: "json",
        success: function (result) {
            $('#output').html(result.data);
            $('#primes').html(result.answer);
        },
        error: function (code, exception) {
            //alert("CRITICAL ERROR, please back away, computer will trigger nuclear meltdown in 5 seconds.");
            alert(code.statis + '|' + exception );
        }
    });
}