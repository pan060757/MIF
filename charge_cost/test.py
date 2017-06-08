a=list((1,2,34))
b=list((2,3,4))
a.extend(b)
print(a)

avgWage=45965.2472
working=297982
retired=165888
floor=3.0
ceil=3
ratio=9
charge1 = ((
           working * 0.69 / 10000 * avgWage + working * 0.30 / 10000 * avgWage * floor + working * 0.01 / 10000 * avgWage * ceil) * (
           ratio - 2) / 100 + avgWage * 0.04 / 10000 * retired) * 0.7
print(charge1)