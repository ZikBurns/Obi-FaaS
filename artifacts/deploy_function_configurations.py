import sys
sys.path.insert(0, '..')
from obifaas.obifaas import Obi_FaaS
if __name__ == '__main__':
    MEMORIES = [1769, 3008, 7076, 10240]
    MEMORIES = [3008]


    for memory in MEMORIES:
        obi = Obi_FaaS(fexec_args={'runtime': f'obifaas_311_{memory}', 'runtime_memory': memory, 'ephemeral_storage': 2048}, ec2_host_machine=False, initialize=False)
        obi.delete_runtime()
        if obi.check_runtime_status():
            print("Runtime is available")
        else:
            print("Runtime is not available")
            obi.redeploy_runtime()
