Program to sort a large (GB size) csv file by some given string columns.

The approach used is:

    a) Split the file into smaller files. One can choose how many files to split.
    b) Sort each smaller file by the given columns.
    c) Merge all the sorted files into one. It uses the concept of merging K sorted lists, 
    picking from the first elements.

Build and run:
    
    $ ./build.sh
    $ sortlargefile -f ./file-small.csv -n 3 -c 1 4 5

Tested with:

    https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Nov.csv
    file size 9,006,762,395 bytes ~ 9 GB

    // rows look like
    event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
    2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,
    4d3b30da-a5e4-49df-b1a8-ba5943f1dd33
    2019-11-01 00:00:00 UTC,view,5000088,2053013566100866035,appliances.sewing_machine,janome,293.65,530496790,
    8e5f4f83-366c-4f70-860e-ca7417414283

    // the full file downloaded above
    String nonSortedFileName = "file.csv";

    // sorted by columns 1, 4, 5 which would be:
    event_type, category_code, brand
    int[] columns = {1, 4, 5};

    // if split in 20 files
    int splitIntoNumFiles = 20;
    a) Time to split file: 38.0 sec
    b) Time to sort files: 286.0 sec
    c) Time to merge: 109.0 sec
    Total time: 433.0 sec

    // if split in 50 files
    int splitIntoNumFiles = 50;
    a) Time to split file: 45.0 sec
    b) Time to sort files: 269.0 sec
    c) Time to merge: 168.0 sec
    Total time: 482.0 sec
