'''
Created on May 9, 2013
'''

import json
import pygraphviz as pgv

class OperatorInfo(object):
    def __init__(self, elemName, hasInput= False):
        self.elemName = elemName
        self.hasInput = hasInput

class StageType(object):
    def __init__(self, iName, dName, opTreeElems=[], linkTrees=True):
        self.internalName = iName
        self.displayName = dName
        self.opTreeElems = opTreeElems
        self.linkOpTrees = linkTrees
        
STAGE_TYPES = {
    "Map Reduce" : StageType("Map Reduce", "MR", [OperatorInfo("Alias -> Map Operator Tree:", True), OperatorInfo("Reduce Operator Tree:")], True),
    "Map Reduce Local Work" : StageType("Map Reduce Local Work", "MR Local",  [OperatorInfo("Alias -> Map Local Operator Tree:", True)]),
    "Fetch Operator" : StageType("Fetch Operator", "Fetch"),
    "Conditional Operator": StageType("Conditional Operator", ""),    
    }

def getStageType(key):
    return STAGE_TYPES[key]

class StageAttributes:
    DEPENDENT_STAGES = "DEPENDENT STAGES" 
    ROOT_STAGE = "ROOT STAGE"
    CONDITIONAL_CHILD_TASKS = "CONDITIONAL CHILD TASKS"
    BACKUP_STAGE = "BACKUP STAGE"
    
class OperatorType(object):
    def __init__(self, name, shortName, infoAttrs=[]):
        self.name = name
        self.shortName = shortName
        self.infoAttrs = infoAttrs
        
OPERATOR_TYPES = {
        "INP" : OperatorType("Input", "INP"),
        "FIL" : OperatorType("Filter", "FIL"),
        "SEL" : OperatorType("Select", "SEL"),
        "FOR" : OperatorType("Forward", "FOR"),
        "FS" : OperatorType("FileSink", "FS"),
        "OP" : OperatorType("Collect/DummyStore", "OP"),
        "SCR" : OperatorType("Script", "SCR"),
        "PTF" : OperatorType("PTF", "PTF"),
        "RS" : OperatorType("ReduceSink", "RS"),
        "EX" : OperatorType("Extract", "EX"),
        "GBY" : OperatorType("GroupBy", "GBY"),
        "JOIN" : OperatorType("Join", "JOIN"),
        "MAPJOIN" : OperatorType("MapJoin", "MAPJOIN"),
        #"MAPJOIN" : OperatorType("SMBMapJoin", "MAPJOIN"),
        "LIM" : OperatorType("Limit", "LIM"),
        "TS" : OperatorType("TableScan", "TS"),
        "UNION" : OperatorType("Union", "Union"),
        "UDTF" : OperatorType("UDTF", "UDTF"),
        "LVJ" : OperatorType("LateralViewJoin", "LVJ"),
         "LVF" : OperatorType("LateralViewForward", "LVF"),
        "HASHTABLEDUMMY" : OperatorType("HashTableDummy", "HASHTABLEDUMMY"),
        "HASHTABLESINK" : OperatorType("HashTableSink", "HASHTABLESINK")
       }

def operatorType(key):
    for k in OPERATOR_TYPES.keys():
        if(key.startswith(k)):
            return OPERATOR_TYPES[k]
    return None

class Operator(object):
    def __init__(self, name, typ):
        if (name.startswith("hdfs:")):
            path = name.lower().split("/")
            self.name = path[-1]
        else:
            self.name = name.lower()
        self.type = typ
        self.children = []
        self.parents = []
        
    def addChild(self, cOp):
        self.children.append(cOp)
        cOp.parents.append(self)
        
    def getEndOfChain(self):
        if (not self.children):
            return self
        return self.children[0].getEndOfChain()
    
    def addToGraph(self, stages, G, nodeNames):
        G.add_node(self.name)
        nodeNames.append(self.name)
        for c in self.children:
            c.addToGraph(stages, G, nodeNames)
            
    def addToStageNode(self, stgNodeLabel, indent='', addChildren=True):
        k = self.name.rfind(".")
        stgNodeLabel += '\n<tr><td align="left">%s%s%s</td></tr>' % (indent, self.name[:k], "..." if not addChildren else "")
        if (addChildren):
            for c in self.children:
                stgNodeLabel = c.addToStageNode(stgNodeLabel, indent + '  ', c.parents[0] is self)                
        return stgNodeLabel

class Stage(object):
    id = 0
    def __init__(self, name):
        self.type = None
        self.id = Stage.id
        Stage.id += 1
        self.name = name
        self.parents = []
        self.children = []
        self.isRoot = False
        self.isConditional = False
        self.backupStage = None
        self.conditionalTasks = []
        self.rootOps = []
        
    def addRootOperator(self, rootOp, linkOpTreesSuppress=False):
        if (self.rootOps and self.type.linkOpTrees and not linkOpTreesSuppress):
            for c in self.rootOps:
                c.getEndOfChain().addChild(rootOp)
        else:
            self.rootOps.append(rootOp)
        
    def setPlan(self, stgContents):
        self.type = getStageType(stgContents.keys()[0])
        self.name = self.name + "(" + self.type.displayName + ")"
        stgContents = stgContents.values()[0]
        for opElem in self.type.opTreeElems:
            if (opElem.elemName in stgContents):
                opContent = stgContents[opElem.elemName]
                currOp = None
                if (opElem.hasInput):
                    for k, v in opContent.items():
                        oper = Operator(k + "." + str(self.id), OPERATOR_TYPES["INP"])
                        self.addRootOperator(oper, True)
                        currOp = oper
                        opContent = v
                        self.buildOpTree(currOp, opContent) 
                else:
                    self.buildOpTree(currOp, opContent)           
        
    def buildOpTree(self, currOp, opContent): 
        for name, childContent in opContent.items():
            opType = operatorType(name) 
            if (opType):
                oper = Operator(name + "." + str(self.id), opType)
                if currOp:
                    currOp.addChild(oper)
                else:
                    self.addRootOperator(oper)   
                self.buildOpTree(oper, childContent)   
        
    def build(self, stages, stageAttrs):
        if (StageAttributes.DEPENDENT_STAGES in stageAttrs):
            cStgs = [x.strip() for x in stageAttrs[StageAttributes.DEPENDENT_STAGES].split(",")]
            for c in cStgs:
                if (not(c in stages)):
                    stages[c] = Stage(c)
                stages[c].children.append(self.name)
                self.parents.append(c)
        self.isRoot = StageAttributes.ROOT_STAGE in stageAttrs and stageAttrs[StageAttributes.ROOT_STAGE]
        if (StageAttributes.CONDITIONAL_CHILD_TASKS in stageAttrs):
            self.isConditional = True
            cStgs = [x.strip() for x in stageAttrs[StageAttributes.CONDITIONAL_CHILD_TASKS].split(",")]
            for c in cStgs:
                if (not (c in stages)):
                    stages[c] = Stage(c)
                self.conditionalTasks.append(c)
        if (StageAttributes.BACKUP_STAGE in stageAttrs):
            bStg = stageAttrs[StageAttributes.BACKUP_STAGE]
            if (not (bStg in stages)):
                    stages[bStg] = Stage(bStg)
            self.backupStage = stages[bStg]
    
    def addToGraph(self, stages, G):
        if (self.isConditional ):
            G.add_node(self.name, shape='diamond')
        else:            
            stgNodeLabel = '<<table BORDER="0" CELLBORDER="1" CELLSPACING="0"><tr><td align="left">%s</td></tr>' % self.name
            for op in self.rootOps:
                stgNodeLabel = op.addToStageNode(stgNodeLabel)
            stgNodeLabel += '</table>>'
            G.add_node(self.name, label=stgNodeLabel, shape='record')           
            
    def addEdge(self, child, G, style=None, label=None):
        G.add_edge(self.name, child.name)
        edge = G.get_edge(self.name, child.name)
        if (label):
            edge.attr['label'] = label
        if (style):
            edge.attr['style'] = style
            
    def addRelsToGraph(self, stages, G):
        for cStg in self.children:
            self.addEdge(stages[cStg],G)
        for cStg in self.conditionalTasks:
            self.addEdge(stages[cStg], G, 'dotted')
        if (self.backupStage):
            self.addEdge(self.backupStage, G, 'dotted', 'backup') 
            
class TopologicalOrder(object):
    ADDED = 1
    CHILDREN_ADDED = 2
    SORTED = 3
    
    def __init__(self, stages):
        self.stages = stages
        self.stageState = {}
        self.stack = self.rootStages(self.stageState)
        
    def rootStages(self, stageState):
        roots = []
        for s in self.stages.values():
            if (s.isRoot):
                roots.append(s)
                stageState[s.name] = TopologicalOrder.ADDED
        return roots
                
    def sort(self):
        sortedList = []
        while(self.stack):
            stg = self.stack.pop()
            state = self.stageState[stg.name]
            if ( state == TopologicalOrder.ADDED ):
                self.stack.append(stg)
                self.stageState[stg.name] = TopologicalOrder.CHILDREN_ADDED
                for c in stg.children:
                    c = self.stages[c]
                    if ( not (c.name in self.stageState)):
                        self.stack.append(c)
                        self.stageState[c.name] = TopologicalOrder.ADDED
                for c in stg.conditionalTasks:
                    c = self.stages[c]
                    if ( not (c.name in self.stageState)):
                        self.stack.append(c)
                        self.stageState[c.name] = TopologicalOrder.ADDED
                if stg.backupStage:
                    c= stg.backupStage
                    if ( not (c.name in self.stageState)):
                        self.stack.append(c)
                        self.stageState[c.name] = TopologicalOrder.ADDED
            else:
                sortedList.append(stg)
        sortedList.reverse()
        return sortedList

def genPlan(planStr,outFile):
    q = json.loads(planStr)
    stages = {}
    for s,sAttrs in q["STAGE DEPENDENCIES"].items():
        stg = stages.get(s)
        if (not stg):
            stg = Stage(s)
            stages[s] = stg
        stg.build(stages, sAttrs)
        stg.setPlan(q["STAGE PLANS"][s])
        
    G=pgv.AGraph(strict=False,directed=True, ranksep=.15, compound=True)
    for stg in stages.values():
        stg.addToGraph(stages, G)
    for stg in stages.values():
        stg.addRelsToGraph(stages, G)
            
    #print G.string()
    G.layout(prog='dot') 
    G.draw(outFile)