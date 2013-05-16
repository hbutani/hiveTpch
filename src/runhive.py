'''
Created on May 6, 2013

'''

import tempfile
import re
import os
import subprocess
import time
from tpchQueries import QUERIES
from hivePlan import genPlan
import optparse
import sys, traceback

'''
Subprocessing in Python: http://docs.python.org/2/library/subprocess.html
'''

def explainQ(hiveCmd, workingDir,db, query, outFile):
    
    if (query.preSql):
        _runQ(hiveCmd,workingDir,db,query.preSql)
    
    f = createExplainScript(db, query.sql)
    try:
        plan = subprocess.check_output([hiveCmd, "-S", "-f", f.name], cwd=workingDir)
        #plan = subprocess.check_output(["/tmp/a"], cwd=workingDir, shell=True)
        genPlan(plan, outFile)
    finally:
        os.unlink(f.name)
        if (query.postSql):
            _runQ(hiveCmd,workingDir,db,query.postSql)

def runQ(hiveCmd, workingDir,db, query):
    sql = "" if not query.preSql else query.preSql
    sql += query.sql
    sql += ("" if not query.postSql else query.postSql)
    return _runQ(hiveCmd, workingDir,db,sql)
        
def _runQ(hiveCmd, workingDir,db, sql):
    f = createRuncript(db, sql)
    try:
        stime = time.time()
        output = subprocess.check_output([hiveCmd, "-S", "-f", f.name], cwd=workingDir)
        etime = time.time()
        return ((etime - stime), output)
        #return subprocess.check_output(["/tmp/a"], cwd=workingDir, shell=True)
    finally:
        os.unlink(f.name)
        
def runQOld(hiveCmd, workingDir,db, query):
    f = createRuncript(db, query)
    try:
        return subprocess.check_output([hiveCmd, "-S", "-f", f.name], cwd=workingDir)
        #return subprocess.check_output(["/tmp/a"], cwd=workingDir, shell=True)
    finally:
        os.unlink(f.name)


def createExplainScript( db,  query):
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write("use %s;\n" % db)
    f.write("explain formatted %s;\n" % query)
    f.close()
    return f

def createRuncript( db,  query):
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write("use %s;\n" % db)
    f.write("%s\n" % query)
    f.close()
    return f

def run(hiveCmd, workingDir, action, queryRegex, db, planDir):
    if planDir and not planDir.endswith("/"):
        planDir += '/'
        
    queryRegex = '^' + queryRegex + '$'
    
    for qName, query in QUERIES.items():
        if re.match(queryRegex, qName):
            try:
                print 'handling query %s' % qName
                if action == 'explain':
                    outFileName = planDir + qName  + ".hplan.png"
                    explainQ(hiveCmd, workingDir,db, query, outFileName)
                elif action == 'execute':
                    res = runQ(hiveCmd, workingDir, db, query)
                    print "%s %d " % (qName, res[0])
            except:
                print "%s on query failed" % action
                traceback.print_exc(file=sys.stdout)

if __name__ == '__main__':
    #hiveCmd = "/media/MyPassport/hadoop/runhive-896-derby.sh"
    #workingDir = "/media/MyPassport/hadoop/hive-896-run"
    hiveCmd = "/Users/i821656/runhive-896-derby.sh"
    workingDir = "/Users/i821656/hive-896-run"
    planDir = '/tmp'
    db = 'tpch0'
    action = 'explain'
    queryRegex = 'q.*'
    
    desc = '''Use this script to execute or generate plans for a set of queries.
    For e.g. to generate a plan for Q1 invoke: 'runhive -a explain -q q1 -d tpch0 -p /tmp -e True'
    '''
    parser = optparse.OptionParser(description=desc)
    
    parser.add_option('-a', '--action', help='query action(execute|explain) to perform [required]', dest='action', type='string', metavar='<ARG>')
    parser.add_option('-q', '--queryPattern', help='which queries to perform action on [required]', dest='queryRegex', type='string', metavar='<ARG>')
    parser.add_option('-d', '--db', help='hive db to use [required]', dest='db', type='string', metavar='<ARG>')
    parser.add_option('-c', '--hiveCommand', help='specify how to run hive [default=%default]', dest='hiveCmd',  default='hive', type='string', metavar='<ARG>')
    parser.add_option('-w', '--workingDir', help='which dir to run hive from [default=%default]', dest='workingDir',  default='.', type='string', metavar='<ARG>')
    parser.add_option('-p', '--planDir', help='dir to output plan img files [default=%default]', dest='planDir',  default='/tmp', type='string', metavar='<ARG>')
    
    (opts, args) = parser.parse_args()
    mandatories = ['db', 'action', 'queryRegex']
    for m in mandatories:
        if not opts.__dict__[m]:
            print "required option '%s' is missing\n" % m
            parser.print_help()
            exit(-1)

    run(opts.hiveCmd, opts.workingDir, opts.action, opts.queryRegex, opts.db, opts.planDir)
        
