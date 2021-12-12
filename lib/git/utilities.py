from datetime import datetime, timedelta
import argparse, os, copy
import pandas as pd
from time_library import hours_worked
# Maximum number of days that is allowed on a submission
MAX_DAYS_WORKED = 3.0
HOURS_IN_A_DAY = 8.0


def get_df(csv_file) :

    try:
        raw_df = pd.read_csv(csv_file, skip_blank_lines=True)
    except Exception as e:
        print(e)
    if len(raw_df.index) < 1:
        raise('No Data Read')
    #print(raw_data.columns)
    # Find if the total hours is recorded in the file

    try:
        idx = raw_df[raw_df[raw_df.columns[0]].str.match('Project')].index.tolist()[0]
        project = raw_df.iloc[idx+1,0].strip()
    except:
        project = ""

    try:
        idx = raw_df[raw_df[raw_df.columns[0]].str.match('Columns')].index.tolist()[0]
        columns = raw_df.iloc[idx+1,0].strip().split(',')
    except:
        columns = []

    # Find where the SVN Logs Start
    try:
        idx = raw_df[raw_df[raw_df.columns[0]].str.match('Git Logs')].index.tolist()[0]
        #print(idx)
    except:
        # No SVN Logs in this one return empty DataFrame
        raw_df = pd.DataFrame()
        return  {'total_hours': -1, 'df': raw_df}

    # Remove the first lines listing the range and hours.
    # And set the only column to be named "data"
    df = raw_df.drop(index=raw_df.index[range(0,idx+1)])
    df.columns = ['data']

    return {'columns': columns, 'project': project, 'df': df}


def get_data(csv_file) :
    """
    Read a CSV file line by line, since it's just a single
    column not much is gained by reading it as csv so
    reading the file.
    """
    project_name = ""
    column_list = []

    with open(csv_file) as fp:
        content = fp.readlines()

    data = []
    row = []
    row_num = 0
    git_logs_found = False; next_line_is_project = False; next_line_is_columns = False
    for line_of_data in content:
        # Find Project Name
        if next_line_is_project:
            project_name = line_of_data.strip()
        if line_of_data.find('Project') >= 0:
            next_line_is_project = True
        else:
            next_line_is_project = False


        # Find the list of Columns
        if next_line_is_columns:
            column_list = line_of_data.split(',')
            column_list = [item.strip() for item in column_list]
        if line_of_data.find('Columns') >= 0:
            next_line_is_columns = True
        else:
            next_line_is_columns = False


        # Find the list of Columns
        if line_of_data.find('Git Logs') >= 0:
            git_logs_found = True
            continue;

        # If we reach here and git logs has not
        # been seen then not time to assemble the logs
        if not git_logs_found: continue
        if row_num < len(column_list):
            if line_of_data.find('2020-') >= 0 or line_of_data.find('2019-')>=0:
                row.append(datetime.strptime(line_of_data.strip(), '%Y-%m-%d'))
            else:
                row.append(line_of_data.strip())
            row_num += 1
        else:
            row.insert(1,project_name)
            data.append(row)
            row = []
            row_num = 0 
    
    column_list.insert(1,'Project')
    #print('PROJECT NAME', project_name)
    #print('COLUMNS FOUND', column_list)
    #for row in data:
    #    print(row)
    df = pd.DataFrame(data, columns=column_list)
    return {'columns': column_list, 'df': df}



def convert_df(orig_df):
    # Convert the vertical SVN style file to a horizontal set of rows
    # Assumes file is in chronologically with most recent first

    # Expected columns names, Branch maybe absent
    # 'Author,', 'Project', 'Commit,', 'Message,', 'Branch', 'Date']
    # Add the number of commits on a given day this will be used
    # to estimate the hours
    orig_df['Num Commits'] = orig_df.groupby(['Name', 'Date']).transform('count')['Commit']
    #print(orig_df.groupby(['Name', 'Date']).agg('count'))
    orig_df = orig_df.sort_values(by='Date')
    orig_df.to_csv('xxx.csv')
    #print("COMMITS", orig_df)

    default_row = { 'Commit ID': "",
                    'Owner': "",
                    'Author': "",
                    'Project': "",
                    'Description': "",
                    'Hours Worked': 0,
                    'Start Date': None,
                    'End Date': None,
                    'Branches': []
    }

    # Since files are in oldest first what we see first is the "next"
    # commit
    prev_commit = None
    rows = []
    for index,row in orig_df.iterrows():
        #print(row)
        new_row = copy.deepcopy(default_row)
        new_row['Commit ID'] = row['Commit']
        new_row['Author'] = row['Name']
        new_row['Desription'] = row['Message']
        new_row['Project'] = row['Project']
                        
        if 'Branch' in orig_df.columns:
            new_row['Branches'].append(row['Branch'])

        if row['Num Commits'] > 1:
            new_row['Hours Worked'] = HOURS_IN_A_DAY / row['Num Commits']
            new_row['Start Date'] = row['Date']
            new_row['End Date'] = row['Date']
        else:
            new_row['End Date'] = row['Date']
            if prev_commit is None:
                new_row['Start Date'] = row['Date']
                new_row['Hours Worked'] = HOURS_IN_A_DAY
            else:
                new_row['Start Date'] = prev_commit['End Date']
                row['Hours Worked'] = hours_worked(new_row['Start Date'], new_row['End Date'])


        rows.append(new_row)
        prev_commit = new_row
    
    #print('COLUMNS', default_row.keys())
    df = pd.DataFrame(rows, columns = default_row.keys())
    #print('DONE', df)

    # Compare the expeted hours with the actual hours if they are not
    # the same then adjust the value
    hours_found = df.agg({'Hours Worked': 'sum'})['Hours Worked']
    print('HOURS BEFORE', hours_found)


    # Now create a new Data Frame
    return  df


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='append', 
                        dest='file', default= None, metavar='filepath',
                        help="CSV file to read the data from")
    parser.add_argument('-d', '--directory', action='store', 
                        dest='dir', default=None, metavar='dirpath',
                        help='Directory containing CSV files to be read.')
    parser.add_argument('-o', '--output', action='store', required=True,
                        dest='out_file', default=None, metavar='output_file',
                        help='File path to store the converted data into.')
    parser.add_argument('--verbose', '-v', action='count', dest='debug_level',
                        help='Set the level of messaging, default is no messages.')
    args = parser.parse_args()

    if args.file is None and args.dir is None:
        parser.error('No input data provided, add --file or -directory')
    if args.file is not None and args.dir is not None:
        parser.error('Too many data inputs provided, use only one of --file or -directory')

    # Not Debug level is not used.
    file_list = []
    if args.file is not None:
        for filepath in args.file:
            file_list.append(os.path.abspath(filepath))
    elif args.dir is not None:
        for filepath in os.listdir(args.dir):
            if os.path.splitext(filepath)[1] != '.csv': continue
            file_list.append(os.path.abspath(os.path.join(args.dir,filepath)))

    return {'output_file': args.out_file, 
            'file_list': file_list}

if __name__ == '__main__':
    args = parse_args()
    print('file list', args['file_list'])

    full_data = None
    for filepath in args['file_list']:
        print(filepath)
        #raw_data = get_df(filepath)
        raw_data = get_data(filepath)
        raw_df = raw_data['df']
        if len(raw_df.index) < 1: continue
        print('Got it', raw_df.head())
        #
        #print(raw_data.head())
        new_df = convert_df(raw_df)

        if full_data is None:
            full_data = new_df
            #print('full', full_data.info())
        else:
            #print('full', full_data.info())
            #print('converted', new_df.info())
            full_data = pd.concat([full_data, new_df], sort=None, copy=True)
    full_data.to_csv(args['output_file'], index=False)
    print("TOTAL HOURS", full_data.agg({'Hours Worked': 'sum'})['Hours Worked'])
