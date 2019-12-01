import numpy as np
import matplotlib.pyplot as plt
import csv
import matplotlib

tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

# Scale the RGB values to the [0, 1] range.
for i in range(len(tableau20)):
    r, g, b = tableau20[i]
    tableau20[i] = (r / 255., g / 255., b / 255.)

map_colors = {
    'OakMap':{'color': tableau20[0], 'marker':'o'},
    'I^2-Oak':{'color': tableau20[0], 'marker':'o'},
    'JavaSkipListMap':{'color': tableau20[2], 'marker':'X'},
    'I^2-legacy':{'color': tableau20[2], 'marker':'X'},
    'YoniList2':{'color': tableau20[4], 'marker':'v'},
    'Raw data': {'color': 'k', 'marker':'v'}
}



map_names = {'OakMap': 'Oak',
             'JavaSkipListMap':'SkipList-OnHeap',
             'YoniList2':'SkipList-OffHeap',
             'OakIncrementalIndex': 'OakIncrementalIndex',
             'IncrementalIndex': 'IncrementalIndex',
             'DataSize':'DataSize'
        
}
linewidth = 5
markersize = 13
myfontsize = 15



def scan_10k():
    benchmarks = []
    tests = []
    maps = []

    with open('summary.csv') as csv_file:
        next(csv_file)
        for row in csv_file:
            if '10k' not in row: continue
            line = row.split(',')
            if len(line) == 7:
                benchmarks.append(line)
                if line[0] not in tests:
                    tests.append(line[0])
                if line[1].strip() not in maps:
                    maps.append(line[1].strip())    


    fig, ax = plt.subplots()
    # ax.set(title=test)
    ax.set_xlabel('Threads', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)

    #value iterator:
    test_lines = [l for l in benchmarks if 'entries' not in l[0]]        
    for mapp in maps:
        line = [m for m in test_lines if m[1].strip() == mapp]
        x = [int(x[4]) for x in line]
        y = [1000*float(x[6].split(' ')[1]) for x in line]
        ax.plot(x, y, label=map_names[mapp] + '-values', color=map_colors[mapp]['color'],
                marker=map_colors[mapp]['marker'],
            linewidth=linewidth,
                markersize=markersize)

    test_lines = [l for l in benchmarks if 'entries' in l[0]]        
    for mapp in maps:
        line = [m for m in test_lines if m[1].strip() == mapp]
        x = [int(x[4]) for x in line]
        y = [1000*float(x[6].split(' ')[1]) for x in line]
        ax.plot(x, y, label=map_names[mapp] + '-entries', color=map_colors[mapp]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                linestyle='--',
                markersize=markersize)

        
    ax.legend(loc='upper left', fontsize=myfontsize)
    fig.tight_layout()
    plt.savefig('ascend_10k.pdf', bbox_inches='tight')                    

def microBenchChart():

    benchmarks = []
    tests = []
    maps = []

    with open('summary.csv') as csv_file:
        next(csv_file)
        for row in csv_file:
            line = row.split(',')
            if len(line) == 7:
                benchmarks.append(line)
                if line[0] not in tests:
                    tests.append(line[0])
                if line[1].strip() not in maps:
                    maps.append(line[1].strip())                    

    for combined in ['get-only', 'ascend-only', 'descend-only', 'ascend10-only']:
        gets = list(filter(lambda x: x == combined or x == 'zc-'+combined, tests))
        fig, ax = plt.subplots()
        # ax.set(title=combined)
        ax.set_xlabel('Threads', fontsize=myfontsize)
        ax.set_ylabel('Kops/sec', fontsize=myfontsize)
        ax.grid()
        for label in ax.get_yticklabels() + ax.get_xticklabels():
            label.set_fontsize(myfontsize)
        test_lines = [l for l in benchmarks if l[0] in gets]        
        for mapp in maps:
            for zc in ['zc-'+combined, combined]:
                if mapp == 'JavaSkipListMap' and 'zc' not in zc:
                    continue
                line = [m for m in test_lines if m[1].strip() == mapp and zc ==  m[0]]
                if len(line) == 0: continue
                x = [int(x[4]) for x in line]
                y = [1000*float(x[6].split(' ')[1]) for x in line]
                label = map_names[mapp] if 'zc' in zc else map_names[mapp] + '-copy'
                linestyle = '-' if 'zc' in zc else '--'
                ax.plot(x, y, label=label, linestyle=linestyle,
                        color=map_colors[mapp]['color'],
                        marker=map_colors[mapp]['marker'],
                        linewidth=linewidth,
                        markersize=markersize)
        # ax.plot([], [], label='zc', linestyle='--', color='k')
        y_bottom = 0
        y_top = ax.get_ylim()[1]
        ax.set_ylim(y_bottom, y_top)                    
        ax.legend(loc='upper left', fontsize=myfontsize)
        fig.tight_layout()
        plt.savefig('micro_combined_' +combined + '.pdf', bbox_inches='tight')
                    
    for test in tests:
        fig, ax = plt.subplots()
        # ax.set(title=test)
        ax.set_xlabel('Threads', fontsize=myfontsize)
        ax.set_ylabel('Kops/sec', fontsize=myfontsize)
        ax.grid()
        for label in ax.get_yticklabels() + ax.get_xticklabels():
            label.set_fontsize(myfontsize)
        test_lines = [l for l in benchmarks if l[0] == test]
        
        for mapp in maps:
            line = [m for m in test_lines if m[1].strip() == mapp]
            x = [int(x[4]) for x in line]
            y = [1000*float(x[6].split(' ')[1]) for x in line]
            ax.plot(x, y, label=map_names[mapp], color=map_colors[mapp]['color'],
                    marker=map_colors[mapp]['marker'],
                    linewidth=linewidth,
                    markersize=markersize)
        ax.legend(loc='upper left', fontsize=myfontsize)
        y_bottom = 0
        y_top = ax.get_ylim()[1]
        ax.set_ylim(y_bottom, y_top)            
        fig.tight_layout()
        plt.savefig('micro_'+test+'.pdf', bbox_inches='tight')
    # plt.show()

def rev_ingestion():
    data = '''26 22 18 16 14
OakMap 147964.84637631665 121731.21498937608 114001.10159319972 109590.93172316228 92637.12989988178
JavaSkipListMap 90447.07432754223 89855.96203590251 101564.68286821648 86271.84701643465 0
YoniList2 77313.32820238254 94738.05423432289 87726.11634264885 78903.10827735398 76235.86181866174'''

    fig, ax = plt.subplots()
    # ax.set(title='Ingestion')
    ax.set_xlabel('GB', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()

    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)
        


    for line in data.split('\n')[1:]:
        mapp = line.split(' ')[0]
        x = [int (i) for i in data.split('\n')[0].split(' ')]
        y = [float(x)/1000 for x in line.split(' ')[1:]]
        if y[-1] == 0:
            x = x[:-1]
            y = y[:-1]            

        ax.plot(x, y, label=map_names[mapp], color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    y_bottom = 0
    y_top = ax.get_ylim()[1]
    ax.set_ylim(y_bottom, y_top)
    
    ax.legend(loc='lower right', fontsize=myfontsize)
    fig.tight_layout()
    plt.savefig('rev_ingestion.pdf', bbox_inches='tight')    
    
def ingestion():

    data = {'32g':
            {
                'OakMap': {'x':[1000000	,5000000,	10000000,	15000000,	20000000, 25000000],
                           'y':'184398.9085584986 148044.08907487668 142887.32089475312 117827.88654974074 120122.67788444876 0'.split()},
                'JavaSkipListMap': {'x':[1000000	,5000000,	10000000,	15000000,	16000000],
                                    'y':'165820.66323476596 88756.34082325075 85367.89268821443 81500.45265893031 0.0'.split()},
                'YoniList2': {'x':[1000000	,5000000,	10000000,	15000000,	20000000, 25000000],
                              'y':'164603.82740996077 119146.20767921317 99665.03706153437 82852.0140768167 71755.84563437132 0'.split()}
            }
    }
    
    fig, ax = plt.subplots()
    # ax.set(title='Ingestion')
    ax.set_xlabel('GB', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)

    for mapp in data['32g']:
        x = [i*(1100)/1000000000 for i in data['32g'][mapp]['x']]
        y = [float(x)/1000 for x in data['32g'][mapp]['y']]
        ax.plot(x[:-1], y[:-1], label=map_names[mapp], color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    y_bottom = 0
    y_top = ax.get_ylim()[1]
    ax.set_ylim(y_bottom, y_top)
    
    ax.legend(loc='lower left', fontsize=myfontsize)
    fig.tight_layout()
    plt.savefig('ingestion.pdf', bbox_inches='tight')
    # plt.show()


def put_scan():

    data = '''1 2 4 8 12
OakMap 42168.23052949118 70100.66498891685 140452.42579290346 233678.36608169592 351618.2024232122
JavaSkipListMap 33775.337077715376 80373.72710454826 154648.8225196247 346708.6479009383 488367.0149825842
YoniList2 41917.566666666666 95477.13333333333 174509.25817903035 336633.63303694135 486691.04398516164'''
    
    
    fig, ax = plt.subplots()
    # ax.set(title='put+scan')
    ax.set_xlabel('Threads', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()

    x = [int (i) for i in data.split('\n')[0].split(' ')]
        
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)
    for line in data.split('\n')[1:]:
        y = [float(x)/1000 for x in line.split(' ')[1:]]
        mapp = line.split(' ')[0]

        ax.plot(x, y, label=map_names[mapp], color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    
    ax.legend(loc='upper left', fontsize=myfontsize)
    fig.tight_layout()
    plt.savefig('put_scan.pdf', bbox_inches='tight')


def put_get():

    data = {'64g':
            {
                'OakMap': {'x':[1,2,4,8,12],
                           'y':'171226.92955117417 384689.83850269165 715283.8405386487 1260801.4732842238 1740945.7045246228'.split()},
                'JavaSkipListMap': {'x':[1,2,4,8,12],
                                    'y':'82577.65703904936 202594.22342960953 419491.9918001367 788003.0498983366 982587.8451243113'.split()},
                'YoniList2': {'x':[1,2,4,8,12],
                              'y':'84240.91265145582 195326.69455509077 370747.387543541 729556.572999288 1056140.5408986271'.split()}
            }
    }
    
    fig, ax = plt.subplots()
    # ax.set(title='put+scan')
    ax.set_xlabel('Threads', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)    
    for mapp in data['64g']:
        x = data['64g'][mapp]['x']
        y = [float(x)/1000 for x in data['64g'][mapp]['y']]
        ax.plot(x, y, label=map_names[mapp], color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    
    ax.legend(loc='upper left', fontsize=myfontsize)
    fig.tight_layout()
    plt.savefig('put_get.pdf', bbox_inches='tight')


def druid_mem_usage_total():

    data = {
        'Raw data': {'x': [1,2,3,4,5,6,7],
                       'y': '1.220703125	2.44140625	3.662109375	4.8828125	6.103515625	7.32421875	8.544921875'.split() },
        'I^2-Oak': {'x': [1,2,3,4,5,6,7],
                                'y': '1.466528969	2.712560249	3.958587517	5.204616631	6.450643578	7.696672989	8.942698799'.split()},
        'I^2-legacy':{'x': [1,2,3,4,5,6,7],
                            'y':'1.684409302	3.348346901	5.004318001	6.676314415	8.332218056	9.988312111	11.67620324'.split()}
        }
    
    druid_name_to_oak = {}
    fig, ax = plt.subplots()
    # ax.set(title='put+scan')
    ax.set_xlabel('Tuples (millions)', fontsize=myfontsize)
    ax.set_ylabel('RAM utilization, GB', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)
        
    for mapp in data:
        x = data[mapp]['x']
        y = [float(i) for i in data[mapp]['y']]
        ax.plot(x, y, label=mapp, color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    ax.legend(loc='lower right', fontsize=myfontsize)
    y_bottom = 0
    y_top = ax.get_ylim()[1]
    ax.set_ylim(y_bottom, y_top)
    fig.tight_layout()
    plt.savefig('druid_mem_usage_total.pdf', bbox_inches='tight')




def druid_ingest_30g():
    data = {
       'I^2-Oak': {'x': [1,2,3,4,5,6,7],
                   'y': '70541.28857	74539.05855	66495.61012	71783.10554	67307.77318	63085.79298	72886.88431'.split()},
        'I^2-legacy':{'x': [1,2,3,4,5,6,7],
                      'y':'68902.11097	63365.48831	58831.27297	55980.88348	49730.62776	47935.52819	32420.76231'.split()}
       }
  
    druid_name_to_oak = {}
    fig, ax = plt.subplots()
    # ax.set(title='put+scan')
    ax.set_xlabel('Tuples (millions)', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)        

    for mapp in data:
        x = data[mapp]['x']
        y = [float(i.replace(',',''))/1000 for i in data[mapp]['y']]
        ax.plot(x, y, label=mapp, color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    ax.legend(loc='lower left', fontsize=myfontsize)
    y_bottom = 0
    y_top = ax.get_ylim()[1]
    ax.set_ylim(y_bottom, y_top)    
    fig.tight_layout()
    plt.savefig('druid_ingest_30g.pdf', bbox_inches='tight')
    


    

def druid_ingest():

 #   data = {
 #       'CII-Oak': {'x': [24,25,26,27,28,29,30],
 #                               'y': '3,285.28	4,489.94	42,026.81	50,202.86	53,219.11	54,479.41	54,423.80'.split()},
 #      'CII-legacy':{'x': [28,29,30],
 #                           'y':'20,576.29 	27,166.61	29,846.70'.split()}
  #      }
    
    data = {
       'I^2-Oak': {'x': [25,26,27,28,29,30, 31, 32],
                               'y': '4.48994	42.02681	50.20286	53.21911	54.47941	54.42380 55.01533 56.11751'.split()},
      'I^2-legacy':{'x': [28,29,30, 31, 32],
                           'y':'20.57629	27.16661	29.84670 33.00082 33.85235'.split()}
       }
  
    druid_name_to_oak = {}
    fig, ax = plt.subplots()
    # ax.set(title='put+scan')
    ax.set_xlabel('Available RAM, GB', fontsize=myfontsize)
    ax.set_ylabel('Kops/sec', fontsize=myfontsize)
    ax.grid()
    for label in ax.get_yticklabels() + ax.get_xticklabels():
        label.set_fontsize(myfontsize)
        

    
    for mapp in data:
        x = data[mapp]['x']
        y = [float(i.replace(',','')) for i in data[mapp]['y']]
        ax.plot(x, y, label=mapp, color=map_colors[mapp.strip()]['color'],
                marker=map_colors[mapp]['marker'],
                linewidth=linewidth,
                markersize=markersize)
    ax.legend(loc='lower right', fontsize=myfontsize)
    y_bottom = 0
    y_top = ax.get_ylim()[1]
    ax.set_ylim(y_bottom, y_top)    
    fig.tight_layout()
    plt.savefig('druid_ingest.pdf', bbox_inches='tight')



    

def main():
    # microBenchChart()
    # scan_10k()
    # ingestion()
    # put_scan()
    # put_get()
    # rev_ingestion()
    druid_mem_usage_total()
    druid_ingest()
    druid_ingest_30g()
    plt.show()


if __name__ == "__main__":
    main()
