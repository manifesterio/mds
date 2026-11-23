import json,glob,re,os,collections,subprocess
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
R='/Users/arun.lakshman/workspace/manifester/mds-verify-slim'
# test name -> file(s) across repo (name collisions possible across pkgs; keep list)
tn=collections.defaultdict(set)
for f in subprocess.check_output(['git','ls-files','*_test.go'],cwd=R,text=True).split():
    for n in re.findall(r'^func ((?:Test|Fuzz)\w+)\(',open(R+'/'+f).read(),re.M): tn[n].add(f)
res={}
for f in glob.glob(S+'/mut_results_w*.jsonl'):
    for l in open(f): r=json.loads(l); res[r['id']]=r
names={}
for f in glob.glob(S+'/mut_names_w*.jsonl'):
    for l in open(f): r=json.loads(l); names[r['id']]=r
files_killing=collections.defaultdict(set); sole=collections.defaultdict(list)
rows=[]
for i,n in names.items():
    r=res[i]; ls=r['layers']
    other=[k for k,v in ls.items() if k!='unit' and v['killed']]
    fs=set()
    for t in n['tests']:
        for f in tn.get(t,()):
            # only count files in the module/pkg directories the unit layer ran
            fs.add(f)
    for f in fs: files_killing[f].add(i)
    unk = len(n['tests'])==0
    rows.append((i,fs,other,n['tests'],unk))
# file is "sole" for mutant i if i's killers are exactly files {f} and no other layer
for i,fs,other,tests,unk in rows:
    if not other and len(fs)==1: sole[list(fs)[0]].append(i)
# group by dir (file same package may share names)
print('mutants with unit-kill attributed:',len(rows),' with no parsed test names (panic/build):',sum(1 for r in rows if r[4]))
print('\nFILE  kills  sole-kills(no other test file or layer kills it)')
for f in sorted(files_killing,key=lambda f:(-len(files_killing[f]))):
    print(f'{len(files_killing[f]):3d} {len(sole.get(f,[])):3d}  {f}  {sole.get(f,[])}')
# unit-killed mutants where other layers also kill -> unit file not needed for them
