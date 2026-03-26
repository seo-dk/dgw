output_path="/test/idcube_out/db=o_tango_a/tb=ue_call_log_union/dt=20251030/hm=1900"
echo "$output_path"
echo "$output_path" | grep -oP "(?<=hh=)[^/]+"
echo "$output_path" | grep -oP "(?<=hm=)[^/]+"
