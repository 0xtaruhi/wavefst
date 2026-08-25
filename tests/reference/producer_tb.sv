module producer_tb;
    logic clock = 0;
    logic reset_n = 0;
    logic [31:0] counter = 0;
    logic signed [15:0] signed_value = -1;
    real analog_value = 0.0;
    string message = "initial";

    always #1 clock = ~clock;
    always @(posedge clock) begin
        if (!reset_n)
            counter <= 0;
        else
            counter <= counter + 1;
    end

    initial begin
        $dumpfile("producer.fst");
        $dumpvars(0, producer_tb);
        #2 reset_n = 1;
        #2 signed_value = -32768;
        #2 analog_value = 3.141592653589793;
        #2 message = "with spaces and symbols !?";
        #8 $finish;
    end
endmodule
