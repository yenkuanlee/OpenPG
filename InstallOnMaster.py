import os
def O(s):
        os.system(s)

O("sudo apt-get install git -y")
O("sudo apt-get install make -y")
O("sudo aptitude install python-pycurl -y")
O("git clone git://github.com/Kozea/Multicorn.git")
print "start to install multicorn..."
os.chdir("Multicorn")
O("dpkg --get-selections | grep hold")
O("sudo aptitude install postgresql-server-dev-9.3 -y")
O("sudo aptitude install build-essential -y")
O("sudo apt-get install build-essential postgresql-server-dev-9.3 python-dev python-setuptools python-pip -y")
O("sudo make && make install")

O("sudo cp ../FDWwrite.py /usr/local/lib/python2.7/dist-packages/multicorn*/multicorn")
O("sudo cp ../Web*.py /usr/local/lib/python2.7/dist-packages/multicorn*/multicorn")
