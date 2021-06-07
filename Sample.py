import matplotlib.pyplot as plt
import numpy as np

y = 10
z=20 
x = np.linspace(0, 20, 100)  # Create a list of evenly-spaced numbers over the range
plt.plot(x, np.sin(x))       # Plot the sine of each x point
plt.show() 

print (y)

