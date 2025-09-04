def igbt_temp_out(igbt_temp_raw):
    igbt_temp_value = 6e-08 *igbt_temp_raw *igbt_temp_raw + 0.0032*igbt_temp_raw - 23.236
    return igbt_temp_value
    
print(igbt_temp_out(10515))
