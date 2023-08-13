import json,argparse,time,commands,os,re,sys
import ROOT as R

hadd_dir = 'hadd' # grid

hadd_tag = 'total'
ups_setup = '. `/cvmfs/gm2.opensciencegrid.org/prod/external/ups/v6_0_7/Linux64bit+3.10-2.17/bin/ups setup jobsub_client`'

def local_test(data_config):
    cmd = 'gm2 -c {0:} -s {1:} -T local_test.root'.format(data_config['fcl'],data_config['test_file'])
    os.system(cmd)

def grid_test(data_config):
    dataset = data_config['dataset'][0]
    output_dir = '{0:}/{1:}_gridTest'.format(data_config['output_dir'],dataset)
    os.system('mkdir -p {0:}'.format(output_dir))

    cmd = '{4:};./gridSetupAndSubmitGM2Data.sh --fhicl {0:} --sam-dataset {1:} --output-dir {2:} {3:} --noGrid --sam-max-files=1 --njobs 1'.format(
        data_config['fcl'],dataset,output_dir,data_config['options'],ups_setup)
    os.system(cmd)

def grid_run(data_config):
    for dataset in data_config['dataset']:
        output_dir = '{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset)
        os.system('mkdir -p {0:}'.format(output_dir))
        n_files = int(commands.getstatusoutput('samweb list-definition-files {0:} | wc -l'.format(dataset))[1])
        if 'njobs' in data_config:
            n_job = data_config['njobs']
        else:    
            n_job = n_files / data_config['nfile_per_job'] + 1

        cmd = '{6:};./gridSetupAndSubmitGM2Data.sh --fhicl {0:} --sam-dataset {1:} --output-dir {2:} {3:} --njobs {4:};sleep {5:}'.format(
        data_config['fcl'],dataset,output_dir,data_config['options'],n_job,data_config['separation_time'],ups_setup)
        os.system(cmd)

def check_status(production_paths):    
    print ('\n{0:<25} {1:>10}/{2:<10} {3:}\n{4:}'.format('project name','finished','total','output path','-'*80))
    for output_dir in production_paths:
        full_path = get_full_path(output_dir)        
        info = get_submission_info(full_path)
        num_expected_jobs = info['expected_jobs']
        num_finished_jobs = get_num_finished_jobs(full_path)
        res = [info['project_name'],full_path,num_expected_jobs,num_finished_jobs]
        print ('{0:<25} {3:>10}/{2:<10} {1:}'.format(*res))


def check_production(data_config):
    production_paths = []
    for dataset in data_config['dataset']:
        production_path = '{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset)
        production_paths.append(production_path)
    check_status(production_paths)

def check_recovery(data_config):
    recovery_paths = []
    for dataset in data_config['dataset']:
        recovery_path = '{0:}/{1:}_recovery'.format(data_config['output_dir'], dataset)
        if (os.path.isdir(recovery_path)):
            recovery_paths.append(recovery_path)
    check_status(recovery_paths)

def check_hadd(data_config):
    hadd_paths = []
    for dataset in data_config['dataset']:
        full_path = get_full_path('{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset))
        hadd_path = '{0:}/{1:}'.format(full_path,hadd_dir)
        hadd_paths.append(hadd_path)
    check_status(hadd_paths)

def hadd(data_config,is_local,final_path=None):
    for dataset in data_config['dataset']:
        full_path = get_full_path('{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset))
        input_dir = '{0:}/{1:}'.format(full_path,'data')
        recovery_path = get_full_path('{0:}/{1:}_recovery'.format(data_config['output_dir'],dataset))
        recovery_dir = '{0:}/{1:}'.format(recovery_path,'data')

        output_dir = '{0:}/{1:}'.format(full_path,hadd_dir)
        os.system('mkdir -p {0:}'.format(output_dir))
        files = commands.getstatusoutput('ls %s/ | wc -l'%(input_dir))[1]
        njob = (int(files) // 350) + 1
        if not is_local:
            cmd = '{3:};./gridSetupAndSubmitGM2Data.sh --hadd --haddmax 400 --tag {0:} --njobs {4:} --disk 20 --input-dir {1:} --output-dir {2:}'.format(hadd_tag,input_dir,output_dir,ups_setup,njob)
        else:
            os.system('mkdir -p {0:}'.format(final_path))
            label = re.findall('_(run[0-9]+[a-zA-Z]+)_', dataset)[0]
            if (os.path.isdir(recovery_dir)):
                cmd = 'hadd -j 10 -f {0:}/{1:}.root {2:}/*.root {3:}/*.root'.format(final_path,label,input_dir,recovery_dir)
            else:
                cmd = 'hadd -j 10 -f {0:}/{1:}.root {2:}/*.root'.format(final_path,label,input_dir)
            print (cmd)
            # return 
        print (cmd)
        os.system(cmd)

def recovery(data_config):
    nfile_per_job = 100
    for dataset in data_config['dataset']:
        full_path = get_full_path('{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset))
        print ('./sam_recovery.sh {0:}'.format(full_path))
        recovery_message = commands.getstatusoutput('./sam_recovery.sh {0:}'.format(full_path))[1]
        print(recovery_message)
        searchObj = re.search(r'SAM recovery dataset created: \'(.*?)\' with (.*?) missing input files.', recovery_message, re.M)
        if searchObj:
            recovery_dataset = searchObj.group(1)
            nfiles = int(searchObj.group(2))

            recovery_path = '{0:}/{1:}_recovery'.format(data_config['output_dir'],dataset)
            njob = nfiles / nfile_per_job + 1
            cmd = '{6:};./gridSetupAndSubmitGM2Data.sh --fhicl {0:} --sam-dataset {1:} --output-dir {2:} {3:} --njobs {4:};sleep {5:}'.format(
            data_config['fcl'],recovery_dataset,recovery_path,data_config['options'],njob,data_config['separation_time'],ups_setup)
            os.system(cmd)
        


def get_full_path(production_path,dateMode='latest'): 
    files = os.listdir(production_path)

    dates = {}
    for date in files:
        if os.path.isdir('%s/%s'%(production_path,date)):
            if re.match('^([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)$',date):
                date_num = int(date.replace('-', ''))              
                dates[date_num] = date
    if len(dates.keys())==0:
        return None

    if dateMode=='latest':
        date = dates[max(dates.keys())]
    elif dateMode=='oldest':
        date = dates[min(dates.keys())]
    else:
        raise ValueError('unknown mode {0:}'.format(dateMode))

    return '%s/%s'%(production_path,date)

def get_submission_info(full_path):
    log = '%s/submission.log'%(full_path)
    
    searchs = {
    'subruns': 'Number of dataset files: ([0-9]+)',\
    'jobs': 'Number of jobs: ([0-9]+)',\
    'isOne': 'One output file \(set\) per input file: (.*)',
    'project_name':'.* --project_name=(\w+) .*'}
    res = {'isOne':'NO','project_name':'None'}

    with open(log,'r') as f:
        for line in f.readlines():
            line = line[:-1]
            for k in searchs:
                if re.match(searchs[k], line):
                    
                    res[k] = re.findall(searchs[k], line)[0]

    if res['isOne'] == 'NO':
        res['expected_jobs'] = int(res['jobs'])
    else:
        res['expected_jobs'] = int(res['subruns'])
    return res

def get_num_finished_jobs(full_path):
    data_path = '%s/data'%(full_path)
    
    
    # files = commands.getstatusoutput('ls %s/*.root | wc -l'%(data_path))[1]
    files = commands.getstatusoutput('ls %s/ | wc -l'%(data_path))[1]
    try:
        N = int(files)
    except:
        N = 0
    return N

def link_files(data_config,linkdir):
    output_dir = '{0:}'
    if linkdir == None:
        linkdir = data_config['output_dir']
    os.system('mkdir -p {0:}'.format(linkdir))

    for dataset in data_config['dataset']:
        full_path = get_full_path('{0:}/{1:}_gridRun'.format(data_config['output_dir'],dataset))
        hadd_path = get_full_path('{0:}/{1:}'.format(full_path,hadd_dir))
        nfiles = int(commands.getstatusoutput('ls {0:}/data/*.root | wc -l'.format(hadd_path))[1])
        label = re.findall('_(run[0-9]+[a-zA-Z]+)_', dataset)[0]
        link_file = '{0:}/{1:}.root'.format(linkdir,label)
        if nfiles==1:
            file_path = commands.getstatusoutput('ls {0:}/data/*.root'.format(hadd_path))[1]
            print('linking {0:}'.format(link_file))
            os.system('ln -s {0:} {1:}'.format(file_path,link_file))
        else:
            file_path = '{0:}/data/*.root'.format(hadd_path)
            print('making {0:}'.format(link_file))
            os.system('hadd -j 8 {1:} {0:}'.format(file_path,link_file))


        
        # print('ln -s {0:} {1:}'.format(file_path,link_file))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--dataset')
    parser.add_argument('-c','--config',default='production.json')
    parser.add_argument('-j','--job',choices=['local_test','grid_test','grid_run','hadd','hadd_local','check_production','check_hadd','link_files','recovery','check_recovery'])
    parser.add_argument('-l','--linkdir',default=None)

    args = parser.parse_args()    

    with open(args.config,'r') as fp:
        data_config = json.load(fp)[args.dataset]

    if args.job == 'local_test':                
        local_test(data_config)
    elif args.job == 'grid_test':
        grid_test(data_config)
    elif args.job == 'grid_run':
        grid_run(data_config)        
    elif args.job == 'check_production':
        check_production(data_config)
    elif args.job == 'hadd':
        hadd(data_config,is_local=False)
    elif args.job == 'hadd_local':
        if args.linkdir == None:
            raise ValueError('use -l xxx to specify the hadd output path')
        hadd(data_config,is_local=True,final_path=args.linkdir)
    elif args.job == 'check_hadd':
        check_hadd(data_config)
    elif args.job == 'link_files':
        link_files(data_config,args.linkdir)
    elif args.job == 'recovery':
        recovery(data_config)
    elif args.job == 'check_recovery':
        check_recovery(data_config)
    else:
        raise ValueError('unknown job {0:}'.format(args.job))
