import lgpio

for i in range(16):
	#h = lgpio.gpiochip_open(0)
	lgpio.gpiochip_open(i)
	lgpio.gpiochip_close(i)
