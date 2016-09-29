system("retz-client load-app -A demo");
10.times do |i|
  system("retz-client schedule -A demo -cmd \"sleep 4\" -cpu 1-1 -mem 512-800");
end
