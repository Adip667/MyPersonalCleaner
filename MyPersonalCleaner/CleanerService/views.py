import configparser
import os

from django.shortcuts import render, redirect
from django.http import HttpResponse
from .cleanResources import run_aws_cleanup
from .azureCleanup import clean_az_rg
from time import strftime


# Create your views here.
def homepage_view(request):
    return HttpResponse('Django says: Hello world!')


def home_view(request):
    time = _get_config('time', 'cleanup')[0]
    config = {}
    config['ec2_cleanup'] = _update_msg('EC2', time)
    config['ebs_cleanup'] = _update_msg('Volumes', time)
    config['ami_cleanup'] = _update_msg('Images', time)
    config['snapshot_cleanup'] = _update_msg('Snapshots', time)
    config['rds_cleanup'] = _update_msg('RDS', time)
    config['rds_snap_cleanup'] = _update_msg('RDS_Snaps', time)

    return render(request, 'Home.html',{'nbar': 'Home','config':config})


def cleanup(request):
    dry_run = True
    EC2 = False
    Volumes = False
    Snapshots = False
    Images = False
    SG = False
    RDS = False
    RDS_Snaps = False
    S3_Objects = False
    Azure_RG = False

    account = ''

    if request.method == 'POST':

        if request.POST.getlist("Runoption")[0] == 'delete':
            dry_run = False

        account = request.POST.getlist("accounts")[0]

        targets = request.POST.getlist("targets")
        if 'ec2_ebs' in targets or 'all' in targets:
            EC2 = True
            Volumes = True

        if 'ami_snaps' in targets or 'all' in targets:
            Snapshots = True
            Images = True

        if 'sg' in targets or 'all' in targets:
            SG = True

        if 'rds_snaps' in targets or 'all' in targets:
            RDS = True
            RDS_Snaps = True

        if 'S3 Objects' in targets or 'all' in targets:
            S3_Objects = True

        if 'Azure RG' in targets or 'all' in targets:
            Azure_RG = True

        xlsx_name = strftime('ResourcesCleaner_' + account + '_' + "%Y-%b-%d_%H-%M-%S.xlsx")
        if dry_run:
            xlsx_name = 'DryRun_' + xlsx_name


        if EC2 or Volumes or Snapshots or Images or SG or RDS or RDS_Snaps or S3_Objects:
            if account == 'Main':
                run_aws_cleanup(xlsx_name, dry_run, EC2, Volumes, Snapshots, Images, SG, RDS, RDS_Snaps, S3_Objects)

            elif account == 'Second':
                run_aws_cleanup(xlsx_name, dry_run, EC2, Volumes, Snapshots, Images, SG, RDS, RDS_Snaps, S3_Objects,
                                'Second')
            else:
                run_aws_cleanup(xlsx_name, dry_run, EC2, Volumes, Snapshots, Images, SG, RDS, RDS_Snaps, S3_Objects)
                run_aws_cleanup(xlsx_name, dry_run, EC2, Volumes, Snapshots, Images, SG, RDS, RDS_Snaps, S3_Objects,
                                'Second', False)

        if Azure_RG:
            clean_az_rg(xlsx_name, dry_run)

    response = HttpResponse(open(f"{xlsx_name}", 'rb').read())
    response['Content-Type'] = 'text/csv'
    response['Content-Disposition'] = f'attachment; filename={xlsx_name}'
    _delete_file(xlsx_name)
    return response

    #return render(request, 'Home.html',{'nbar': 'Home'})


def _delete_file(path):
    """ Deletes file from filesystem. """
    if os.path.isfile(path):
        os.remove(path)

def _get_config(value, section):
    config = configparser.ConfigParser()
    config.read('config.txt')
    value = [config[section][value]]
    return value

def _update_config(param, value, section):
    config = configparser.ConfigParser()
    config.read('config.txt')
    config.set(section, value, param)

    with open('config.txt', 'w') as configfile:
        config.write(configfile)

def _update_msg(resouorce, time):

    tag_msg ='Delete untagged resources only'
    date_msg ='Delete resources older the {x} days and untagged'.format(x=time)
    config = _get_config(resouorce, 'cleanup')[0]
    if config == 'keeptag': return tag_msg
    else: return date_msg


def configurations(request):
    if request.method == 'POST':
        if request.POST.getlist("aws_account_main"):
            _update_config(request.POST.getlist("aws_account_main")[0],'aws_account','aws_details')

        elif request.POST.getlist("aws_account_second"):
            _update_config(request.POST.getlist("aws_account_second")[0], 'aws_account', 'aws_details_2nd')
            _update_config(request.POST.getlist("aws_role")[0], 'role_to_assume', 'aws_details_2nd')

        elif request.POST.getlist("region_all"):
            _update_config(request.POST.getlist("region_all")[0], 'aws_regions_all', 'general')
            _update_config(request.POST.getlist("regions")[0], 'aws_regions', 'general')
            _update_config(request.POST.getlist("logs_console")[0], 'logs_console', 'general')
            _update_config(request.POST.getlist("logs_file")[0], 'logs_file', 'general')

        elif request.POST.getlist("client_secret"):
            _update_config(request.POST.getlist("client_secret")[0], 'client_secret', 'azure_details')
            _update_config(request.POST.getlist("client_id")[0], 'client_id', 'azure_details')
            _update_config(request.POST.getlist("tenant_id")[0], 'tenant_id', 'azure_details')
            _update_config(request.POST.getlist("subscription_id")[0], 'subscription_id', 'azure_details')

        elif request.POST.getlist("ec2_cleanup"):
            _update_config(request.POST.getlist("ec2_cleanup")[0], 'EC2', 'cleanup')
            _update_config(request.POST.getlist("ebs_cleanup")[0], 'Volumes', 'cleanup')
            _update_config(request.POST.getlist("ami_cleanup")[0], 'Images', 'cleanup')
            _update_config(request.POST.getlist("snapshot_cleanup")[0], 'Snapshots', 'cleanup')
            _update_config(request.POST.getlist("rds_cleanup")[0], 'RDS', 'cleanup')
            _update_config(request.POST.getlist("rds_snap_cleanup")[0], 'RDS_Snaps', 'cleanup')
            _update_config(request.POST.getlist("time_cleanup")[0], 'time', 'cleanup')


    config ={}
    config['aws_account_main']= _get_config('aws_account','aws_details')[0]

    config['aws_account_second'] = _get_config('aws_account', 'aws_details_2nd')[0]
    config['aws_role'] = _get_config('role_to_assume', 'aws_details_2nd')[0]

    config['region_all'] = _get_config('aws_regions_all', 'general')[0]
    config['regions'] = str(_get_config('aws_regions', 'general')[0])
    config['logs_console'] = _get_config('logs_console', 'general')[0]
    config['logs_file'] = str(_get_config('logs_file', 'general')[0])


    config['client_secret'] = _get_config('client_secret', 'azure_details')[0]
    config['client_id'] = _get_config('client_id', 'azure_details')[0]
    config['tenant_id'] = _get_config('tenant_id', 'azure_details')[0]
    config['subscription_id'] = _get_config('subscription_id', 'azure_details')[0]

    config['ec2_cleanup'] = _get_config('EC2', 'cleanup')[0]
    config['ebs_cleanup'] = _get_config('Volumes', 'cleanup')[0]
    config['ami_cleanup'] = _get_config('Images', 'cleanup')[0]
    config['snapshot_cleanup'] = _get_config('Snapshots', 'cleanup')[0]
    config['rds_cleanup'] = _get_config('RDS', 'cleanup')[0]
    config['rds_snap_cleanup'] = _get_config('RDS_Snaps', 'cleanup')[0]
    config['time_cleanup'] = _get_config('time', 'cleanup')[0]

    return render(request, 'config.html',{'nbar': 'Configuration','config':config})
