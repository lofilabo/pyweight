'''
Created on 28 Mar 2019

@author: richard
'''


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def myfunc(self):
        print("Hello my name is " + self.name)



if __name__ == '__main__':

    p1 = Person("John", 36)
    print(p1.name)
    print(p1.age)
    p1.myfunc()