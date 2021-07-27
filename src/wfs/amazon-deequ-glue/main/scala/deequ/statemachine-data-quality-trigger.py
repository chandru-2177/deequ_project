import boto3
import json, sys, datetime, os, getopt
from datetime import datetime

def callStepFn(redshiftSchema, redshiftTables, redShiftSecretRegion, redShiftSecretName):
    try:
        # Define parameters
        ts = datetime.now().strftime('%G%m%d%H%M%S')
        execName = 'Exec-' + redShiftSecretName + '-' + redshiftSchema + '-' + "-".join(redshiftTables.split(',')) + '-' + ts
        inputParam = {"glueDatabase" : redshiftSchema, "glueTables" : redshiftTables, "redShiftSecretRegion" : redShiftSecretRegion, "redShiftSecretName":redShiftSecretName}
        inputJson = json.dumps(inputParam)

        awsAccount=boto3.client('sts').get_caller_identity().get('Account')
        stateMachineArn= 'arn:aws:states:us-west-2:' + awsAccount + ':stateMachine:data-quality-sm'

        # Trigger Step Functions
        sfn = boto3.client('stepfunctions')
        stepExec = sfn.start_execution(
            stateMachineArn=stateMachineArn,
            name=execName,
            input=inputJson
        )
        
        print(f'Deequ is job submitted, {stepExec}')
    except Exception as e:
        raise (e)


def main():
    try:
        #getting the arguments from command line
        argv = sys.argv[1:]
        redshiftSchema, redshiftTables, redShiftSecretRegion, redShiftSecretName = None, None, None, None
        supported_args = ['redshiftSchema=','redshiftTables=','redShiftSecretRegion=','redShiftSecretName=']
        try:
            optlist, remaining = getopt.getopt(argv, "", supported_args)
        except getopt.GetoptError as err:
            print (str(err))

        for arg, value in optlist:
            if arg == '--redshiftSchema':
                redshiftSchema = str(value)
            elif arg == '--redshiftTables':
                redshiftTables = str(value)
            elif arg == '--redShiftSecretRegion':
                redShiftSecretRegion = str(value)
            elif arg == '--redShiftSecretName':
                redShiftSecretName = str(value)            
            else: 
                print("Error in arguments parsing")   

        callStepFn(redshiftSchema, redshiftTables, redShiftSecretRegion, redShiftSecretName)
    except Exception as e:
        raise (e)

if __name__ == "__main__":
    main()