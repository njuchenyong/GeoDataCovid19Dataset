import matplotlib.pyplot as plt
import numpy as np
from scipy.integrate import solve_ivp
from scipy.optimize import minimize

def SEIR(t,y):
    S = y[0]
    E = y[1]
    I = y[2]
    R = y[3]
    return([-beta*S*I,beta*S*I-alpha*E,alpha*E-gamma*I, gamma*I])

#optimize with sum of squares
def sumsq(p):
    alpha,beta, gamma = p
    sol = solve_ivp(SIR, [0, 14], [762,0, 1, 0], t_eval=np.arange(0, 14.2, 0.2))
    return (sum((sol.y - data) ** 2))

#minimize

msol = minimize(sumsq,[0.001,0.01,1],method='Nelder-Mead')