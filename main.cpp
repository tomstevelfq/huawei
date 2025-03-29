#include <bits/stdc++.h>
#include<cstdlib>

using namespace std;

#define MAX_DISK_NUM (10 + 1)          // 硬盘编号 1~N（N最大为10）
#define MAX_DISK_SIZE (16384 + 1)      // 每个硬盘存储单元编号 1~V（V最大为16384）
#define MAX_REQUEST_NUM (30000000 + 1) // 请求数量上限（数组下标从1开始）
#define MAX_OBJECT_NUM (100000 + 1)    // 对象数量上限（数组下标从1开始）
#define REP_NUM 3                      // 每个对象存储3个副本
#define FRE_PER_SLICING 1800           // 每个时间片段长度
#define EXTRA_TIME 105                 // 附加时间片数
#define FIRST_READ 64                  // 第一次读取操作消耗令牌
#define MAX_TAG_NUM 16
int BLOCK_GROUP_SIZE=1024;
map<int,int> mp;
vector<int> siz;
vector<int> fail;
int jum=0;
int jum_all=0;

struct ControlModule;
struct Disk;
struct Zone;
struct Tag;
struct Object;
struct Request;
struct StorageUnit;
struct Block_Group
{
    int selected_type = 0;
    int start_pos; // 包含开始位置
    int end_pos;   // 包含结束位置
    int id;
    int use_size = 0;
    int v;
    Block_Group(int id, int v) : id(id), v(v)
    {
        start_pos = (id - 1) * BLOCK_GROUP_SIZE + 1;
        end_pos = min(id * BLOCK_GROUP_SIZE, v);
    }
};
struct Object
{
    int obj_id;
    int replica[REP_NUM + 1]; //每个副本存在哪个磁盘上
    int *unit[REP_NUM + 1]; // 每个副本中对象块的存储单元索引（1-based）
    int size;
    int tag;
    int last_request_point; // 挂接未完成请求链头
    bool is_delete;
    bool *block_read_status; // 每个对象块是否已被读取（1-based）

    Object() : obj_id(0), size(0), tag(0), last_request_point(0), is_delete(false), block_read_status(nullptr)
    {
        for (int i = 1; i <= REP_NUM; i++)
        {
            unit[i] = new int[MAX_DISK_SIZE];
        }
    }
    ~Object()
    {
        for (int i = 1; i <= REP_NUM; i++)
        {
            if (unit[i])
            {
                delete[] unit[i];
                unit[i] = nullptr;
            }
        }
        if (block_read_status)
        {
            delete[] block_read_status;
            block_read_status = nullptr;
        }
    }
};

struct StorageUnit
{
    int object_id; // 0表示空闲，否则为对象ID
    int block_id;  // 对象块编号（1-based）
    StorageUnit() : object_id(0), block_id(0) {}
};

struct FreeSpace
{
    vector<int> size1;
    vector<int> size2;
    vector<int> size3;
    vector<int> size4;
    vector<int> size5;
    FreeSpace() : size1(), size2(), size3(), size4(), size5() {}
};

struct Disk
{
    int id;
    StorageUnit storage[MAX_DISK_SIZE];
    vector<Zone *> zones; // disk里面存放 zone 的分布，如果没有对象只能存放在 zone 里面，放进去的 zone，下面有一个 create_zone，是在这块磁盘最大的一块没有被zone占据的最中心初始化一个磁盘总大小的 10% 的zone
    int head_position;
    char last_action;
    int last_token_cost;
    int used_tokens;
    int disk_size;
    bool used_read;
    vector<pair<int, int>> done_request;
    vector<int> request;
    FreeSpace freespace;

    //========================新增block_group逻辑
    vector<Block_Group> block_groups;
    int block_group_num;
    void initBlockGroup(int num, int v)
    {
        block_group_num = num;
        block_groups.push_back(Block_Group(0, 0));
        for (int i = 1; i <= block_group_num; i++)
        {
            block_groups.push_back(Block_Group(i, v));
        }
    }

    Disk() : id(0), head_position(1), last_action('\0'), last_token_cost(0),
             used_tokens(0), disk_size(0)
    {
        memset(storage, 0, sizeof(storage));
    }

    Zone *create_zone(int tag, int replica);

    int calculateReadToken(Disk &disk)
    {
        if (disk.last_action != 'r')
        {
            return 64;
        }
        else
        {
            float token = disk.last_token_cost * 0.8f;
            token = ceil(token); // 向上取整
            return max(16, static_cast<int>(token));
        }
    }
};

struct Zone
{
    int disk_id;
    int start; // 起始块编号
    int end;   // 结束块编号
    int tag;   // 所属标签（0表示空闲）
    int used;
    int max_cap;

    Zone(int s, int e, int tag, int disk_id, int max_cap, int left, int right, int replica) // 修正第四个参数类型
        : start(s), end(e), tag(tag), disk_id(disk_id), max_cap(max_cap)
    {
    }

    bool expand_zone(ControlModule *cm, int expand_size);
    int allocate_storage_in_zone(ControlModule *cm, int size, int obj_id, int *units, int extend_size = -1);
};

struct Request
{
    int req_id;
    int object_id;
    int prev_id;
    int arrival_time; // 请求到达时间
    bool is_done;
    float value; // 动态计算的优先级
    bool *block_read_status;

    Request() : req_id(0), object_id(0), prev_id(0), arrival_time(0), is_done(false), value(0.0), block_read_status(nullptr) {}

    ~Request()
    {
        if (block_read_status)
        {
            delete[] block_read_status;
            block_read_status = nullptr;
        }
    }
};

struct Tag
{
    int tag_id;
    int heat;
    int *delete_obj;
    int *write_obj;
    int *read_obj;
    vector<Zone *> zones;
    vector<Disk *> disks;

    Tag() : tag_id(0), heat(0), delete_obj(nullptr), write_obj(nullptr), read_obj(nullptr) {}

    Zone *expandZones(ControlModule *cm, Zone *zone);
};

struct ControlModule
{
    Disk disks[MAX_DISK_NUM];          // 硬盘数组（下标1~N有效）
    Tag tags[MAX_TAG_NUM];             // Tag 数组（1 -16 有效）
    Object objects[MAX_OBJECT_NUM];    // 对象数组（下标1~MAX_OBJECT_NUM-1有效）
    Request requests[MAX_REQUEST_NUM]; // 请求数组（下标1~MAX_REQUEST_NUM-1有效）

    int current_time; // 当前时间片编号
    int N;            // 硬盘数量
    int V;            // 每块硬盘存储单元数量
    int G;            // 每个磁头每个时间片最大令牌数
    int req_num;
    ControlModule() : current_time(0), N(0), V(0), G(0), req_num(0) {}

    void updateRequestTimeouts(int new_request_id);
};

void ControlModule::updateRequestTimeouts(int request_size)
{
    req_num += request_size;
    for (int i = req_num; i >= 1; i--)
    {
        if (!requests[i].is_done)
        {
            if (requests[i].value <= 0)
                break;
            requests[i].value -= 0.01f;
            if (requests[i].value <= 0)
            { // 浮点数用<=判断
                Object &obj = objects[requests[i].object_id];
                for (int block = 1; block <= obj.size; block++)
                {
                    obj.block_read_status[block] = false;
                }
            }
        }
    }
}

bool allocate_storage(Disk &disk, int size, int obj_id, int *units, int V, int from, int to)
{
    // 遍历所有可能的起始位置
    for (int start = from; start <= to - size + 1; )
    {
        bool allFree = true;
        // 检查从 start 开始连续 size 个块是否都空闲
        for (int j = 0; j < size; j++)
        {
            if (disk.storage[start + j].object_id != 0)
            {
                allFree = false;
                start=start+j+1;
                break;
            }
        }
        // 找到一个连续的空闲区域
        if (allFree)
        {
            for (int j = 0; j < size; j++)
            {
                int pos = start + j;
                disk.storage[pos].object_id = obj_id;
                disk.storage[pos].block_id = j + 1;
                units[j + 1] = pos;
            }
            return true;
        }
    }
    return false;
}
void handle_timestamp(ControlModule *cm)
{
    int timestamp;
    scanf("%*s%d", &timestamp);
    cm->current_time = timestamp;
    printf("TIMESTAMP %d\n", timestamp);
    fflush(stdout);
}

void do_object_delete(const int *object_unit, Disk &disk, int size)
{
    //======================更新 block_group
    int block_group_id = object_unit[1] / BLOCK_GROUP_SIZE;
    if (object_unit[1] % BLOCK_GROUP_SIZE != 0)
    {
        block_group_id++;
    }
    Block_Group &block_group = disk.block_groups[block_group_id];
    block_group.use_size -= size;
    if (block_group.use_size == 0)
    {
        block_group.selected_type = 0;
    }
    for (int i = 1; i <= size; i++)
    {
        int unit_index = object_unit[i];
        disk.storage[unit_index].object_id = 0;
        disk.storage[unit_index].block_id = 0;
    }
}

void delete_action(ControlModule *cm)
{
    int n_delete;
    scanf("%d", &n_delete);
    static int _id[MAX_OBJECT_NUM];
    for (int i = 1; i <= n_delete; i++)
    {
        scanf("%d", &_id[i]);
    }

    vector<int> abort_reqs;
    for (int i = 1; i <= n_delete; i++)
    {
        int id = _id[i];
        int current_id = cm->objects[id].last_request_point;
        while (current_id != 0)
        {
            if (!cm->requests[current_id].is_done)
            {
                abort_reqs.push_back(current_id);
                cm->requests[current_id].is_done = true; // 标记为已取消
            }
            current_id = cm->requests[current_id].prev_id;
        }
        for (int rep = 1; rep <= REP_NUM; rep++)
        {
            int disk_id = cm->objects[id].replica[rep];
            if (disk_id < 1 || disk_id > cm->N)
                continue;
            do_object_delete(cm->objects[id].unit[rep], cm->disks[disk_id], cm->objects[id].size);
        }
        cm->objects[id].is_delete = true;
    }

    printf("%d\n", (int)abort_reqs.size());
    for (int req_id : abort_reqs)
    {
        printf("%d\n", req_id);
    }
    fflush(stdout);
}

bool hascontinue(vector<Block_Group> &groups,int b,int size,StorageUnit storage[]){
    int start = groups[b].start_pos;
    int end = groups[b].end_pos;
    for (int j = start; j <= end - size + 1; )
    {
        bool contiguous = true;
        for (int k = 0; k < size; k++)
        {
            if (storage[j + k].object_id != 0)
            {
                contiguous = false;
                j=j+k+1;
                break;
            }
        }
        if (contiguous)
        {
            return true;
        }
    }
    return false;
}

vector<pair<int, int>> select_block_group(int tag, int size, ControlModule *cm)
{

    vector<pair<int, int>> rep_block_group;
    // 先查找是否存在打了tag 的 block_group
    vector<int> all_disks;
    for (int d = 1; d <= cm->N; d++)
        all_disks.push_back(d);
    random_shuffle(all_disks.begin(), all_disks.end());
    vector<int> all_block_groups;
    for (int b = 1; b <= cm->disks[1].block_group_num; b++)
    {
        all_block_groups.push_back(b);
    }
    random_shuffle(all_block_groups.begin(), all_block_groups.end());
    vector<pair<int, pair<int, int>>> satisfy_block_group;
    for (auto d : all_disks)
    {
        if (rep_block_group.size() >= REP_NUM)
        {
            break;
        }
        int max_b = 0;
        int max_size = 0;
        for (auto b : all_block_groups)
        {
            if (cm->disks[d].block_groups[b].selected_type != tag)
            {
                continue;
            }
            bool hasContiguous=hascontinue(cm->disks[d].block_groups,b,size,cm->disks[d].storage);
            if (hasContiguous)
            {
                if (cm->disks[d].block_groups[b].use_size > max_size)
                {
                    max_size = cm->disks[d].block_groups[b].use_size;
                    max_b = b;
                }
            }
        }
        if (max_b != 0)
        {
            satisfy_block_group.push_back(make_pair(d, make_pair(max_b, max_size)));
        }
    }
    std::sort(satisfy_block_group.begin(), satisfy_block_group.end(), [](const auto &a, const auto &b)
              { return a.second.second > b.second.second; });

    for (auto &group : satisfy_block_group)
    {
        if (rep_block_group.size() >= REP_NUM)
        {
            break;
        }
        rep_block_group.push_back(make_pair(group.first, group.second.first));
    }
    satisfy_block_group.clear();

    if (rep_block_group.size() >= REP_NUM)
    {
        mp[1]++;
    }else{
        mp[0]++;
        siz.push_back(size);
    }

    for (auto d : all_disks)
    {
        if (rep_block_group.size() >= REP_NUM)
        {
            break;
        }
        int min_b = 0;
        int min_size = BLOCK_GROUP_SIZE;
        int ma_size=0;
        int ma_b=0;
        bool repeat = false;
        for (auto &rep_disk_id : rep_block_group)
        {
            if (rep_disk_id.first == d)
            {
                repeat = true;
                break;
            }
        }
        if (repeat)
        {
            continue;
        }
        for (auto b : all_block_groups)
        {
            bool hasContiguous=hascontinue(cm->disks[d].block_groups,b,size,cm->disks[d].storage);
            if (hasContiguous)
            {
                if (cm->disks[d].block_groups[b].use_size < min_size)
                {
                    min_size = cm->disks[d].block_groups[b].use_size;
                    min_b = b;
                }
            }
        }
        if (min_b != 0)
        {
            satisfy_block_group.push_back(make_pair(d, make_pair(min_b, min_size)));
        }
    }

    std::sort(satisfy_block_group.begin(), satisfy_block_group.end(), [](const auto &a, const auto &b)
              { return a.second.second < b.second.second; });

    for (auto &group : satisfy_block_group)
    {
        if (rep_block_group.size() >= REP_NUM)
        {
            break;
        }
        rep_block_group.push_back(make_pair(group.first, group.second.first));
    }
    return rep_block_group;
}

void write_action(ControlModule *cm)
{
    int n_write;
    scanf("%d", &n_write);
    for (int i = 0; i < n_write; i++)
    {
        int obj_id, size, tag;
        scanf("%d%d%d", &obj_id, &size, &tag);
        siz.push_back(size);
        if (obj_id < 1 || obj_id >= MAX_OBJECT_NUM)
            continue;

        Object &obj = cm->objects[obj_id];
        obj.obj_id = obj_id;
        obj.size = size;
        obj.tag = tag;
        obj.last_request_point = 0;
        obj.is_delete = false;

        obj.block_read_status = new bool[size + 1]();
        vector<pair<int, int>> rep_block_group = select_block_group(tag, obj.size, cm);
        for (int rep = 1; rep <= REP_NUM; rep++)
        {
            obj.unit[rep] = new int[size + 1];

            Disk &disk = cm->disks[rep_block_group[rep - 1].first];
            Block_Group &block_group = disk.block_groups[rep_block_group[rep - 1].second];
            bool ok = allocate_storage(disk, size, obj_id, obj.unit[rep], cm->V, block_group.start_pos, block_group.end_pos);
            assert(ok);
            obj.replica[rep] = rep_block_group[rep - 1].first;
            if (block_group.selected_type == 0)
            {
                block_group.selected_type = tag;
            }
            block_group.use_size += size;
        }
        printf("%d\n", obj_id);
        for (int rep = 1; rep <= REP_NUM; rep++)
        {
            printf("%d", obj.replica[rep]);
            for (int b = 1; b <= size; b++)
            {
                printf(" %d", obj.unit[rep][b]);
            }
            printf("\n");
        }
    }
    fflush(stdout);
}

void read_action(ControlModule *cm)
{
    int n_read;
    scanf("%d", &n_read);

    for (int i = 0; i < n_read; i++)
    {
        int req_id, obj_id;
        scanf("%d%d", &req_id, &obj_id);
        Request &req = cm->requests[req_id];
        Object &obj = cm->objects[obj_id];
        req.req_id = req_id;
        req.is_done = false;
        req.object_id = obj_id;
        req.arrival_time = cm->current_time;
        req.value = 1.0f;
        if (req.block_read_status)
            delete[] req.block_read_status;
        req.block_read_status = new bool[obj.size + 1]();

        req.prev_id = cm->objects[obj_id].last_request_point;
        cm->objects[obj_id].last_request_point = req_id;

        for (int rep = 1; rep <= REP_NUM; rep++)
        {
            int disk_id = obj.replica[rep];
            cm->disks[disk_id].request.push_back(req_id);
        }
    }

    cm->updateRequestTimeouts(n_read);

    vector<int> completed;
    for (int d = 1; d <= cm->N; d++)
    {
        Disk &disk = cm->disks[d];
        // disk 函数，清理过期请求
    }

    for (int d = 1; d <= cm->N; d++)
    {
        Disk &disk = cm->disks[d];
        string instruction;
        disk.used_tokens = 0;

        for (; cm->G - disk.used_tokens > 0;)
        {
            Object &head_obj = cm->objects[disk.storage[disk.head_position].object_id];
            if (cm->requests[head_obj.last_request_point].value > 0 && head_obj.obj_id != 0)
            {
                // 读read
                Object &obj = cm->objects[disk.storage[disk.head_position].object_id];
                int read_token = disk.calculateReadToken(disk);
                // printf("%d\n", read_token);
                if (cm->G - disk.used_tokens > read_token)
                {
                    instruction += 'r';
                    disk.used_tokens += read_token;
                    disk.last_token_cost = read_token;
                    disk.last_action = 'r';

                    int current_req = obj.last_request_point;
                    while (cm->requests[current_req].value > 0 && !cm->requests[current_req].is_done)
                    {
                        int block_id = disk.storage[disk.head_position].block_id;
                        cm->requests[current_req].block_read_status[block_id] = true;
                        bool req_statu = true;
                        for (int block = 1; block <= obj.size; block++)
                        {
                            if (!cm->requests[current_req].block_read_status[block])
                            {
                                req_statu = false;
                                break;
                            }
                        }
                        if (req_statu)
                        {
                            cm->requests[current_req].value = 0;      // value 置零
                            cm->requests[current_req].is_done = true; // 标记已读
                            completed.push_back(current_req);         // 加入缓存
                        }
                        current_req = cm->requests[current_req].prev_id; // 检查上一个请求
                    }
                    disk.head_position = (disk.head_position % cm->V) + 1;
                }
                else
                {
                    instruction += '#';
                    break;
                }
            }
            // 情况2：当前位置无任务，寻找下一个任务位置
            else
            {
                jum_all++;
                int distance = 0, j;
                for (j = disk.head_position % cm->V + 1; j != disk.head_position; j = (j % cm->V) + 1)
                {
                    ++distance; // 计算移动步数
                    Object &obj = cm->objects[disk.storage[j].object_id];
                    if (cm->requests[obj.last_request_point].value > 0 && disk.storage[j].object_id != 0)
                    {
                        // debug_function(cm, 0, 6, 6, 6);
                        break; // 找到可处理位置
                    }
                }
                if (j == disk.head_position)
                {
                    // debug_function(cm, 0, 9, 9, 9);
                    instruction += '#'; // 结束
                    break;
                }
                // 情况2.1：移动距离超过最大令牌数，且令牌未被使用过
                if (distance >= cm->G)
                {
                    // if (disk.used_tokens == cm->G) { // 可以执行Jump
                    if (disk.used_tokens == 0)
                    {                                     // 可以执行Jump
                        jum++;
                        instruction = "j ";               // 输出Jump指令
                        instruction += std::to_string(j); // 将整数j转换为字符串并拼接
                        disk.head_position = j;
                        disk.last_token_cost = cm->G;
                        disk.last_action = 'j';
                        break;
                    }
                }

                // 情况2.2：移动并消耗令牌
                distance = min(distance, cm->G - disk.used_tokens); // 实际可移动步数
                for (; distance > 0;)
                {
                    instruction += 'p'; // 输出Jump指令
                    disk.last_token_cost = 1;
                    disk.head_position = (disk.head_position % cm->V) + 1; // 移动磁头
                    disk.last_action = 'p';
                    disk.used_tokens++;
                    --distance;
                }

                // 令牌用完则结束
                if (disk.used_tokens == cm->G)
                {
                    instruction += '#';
                    break;
                }
            }
        }
        // printf("%s\n", instruction.c_str());

        printf("%s\n", instruction.c_str());
    }

    // 输出完成请求
    printf("%d\n", (int)completed.size());
    for (int id : completed)
    {
        printf("%d\n", id);
    }
    fflush(stdout);
}
void clean(ControlModule *cm)
{
    fflush(stdout);
}

// ---------------------------
// 主函数
// ---------------------------
int main(int argc,char *argv[])
{
    //BLOCK_GROUP_SIZE=stoi(argv[1]);
    ControlModule *cm = new ControlModule();

    int T, M;
    scanf("%d%d%d%d%d", &T, &M, &cm->N, &cm->V, &cm->G);

    int slices = (T - 1) / FRE_PER_SLICING + 1;

    for (int i = 1; i <= MAX_TAG_NUM; i++)
    {
        cm->tags[i].read_obj = new int[slices];
        cm->tags[i].read_obj = new int[slices];
        cm->tags[i].read_obj = new int[slices];
    }

    for (int i = 0; i < M * 3; i++)
    {
        for (int j = 0; j < slices; j++)
        {
            int val;
            scanf("%d", &val);
            if (i < M)
            {
                int tag_id = i + 1;
                cm->tags[tag_id].read_obj[j] = val;
            }
            else if (i < 2 * M)
            {
                int tag_id = (i - M) + 1;
                cm->tags[tag_id].read_obj[j] = val;
            }
            else
            {
                int tag_id = (i - 2 * M) + 1;
                cm->tags[tag_id].read_obj[j] = val;
            }
        }
    }
    // 预处理阶段结束后输出 "OK"

    printf("OK\n");
    fflush(stdout);

    //=======================初始化block_group_num
    int block_group_num = cm->V / BLOCK_GROUP_SIZE;
    if (cm->V % BLOCK_GROUP_SIZE != 0)
    {
        block_group_num++;
    }
    // 初始化硬盘状态（1-based索引）
    for (int d = 1; d <= cm->N; d++)
    {
        cm->disks[d].id = d;
        cm->disks[d].head_position = 1;
        cm->disks[d].last_action = '\0';
        cm->disks[d].last_token_cost = 0;
        cm->disks[d].used_tokens = 0;
        cm->disks[d].disk_size = cm->V;
        for (int j = 1; j <= cm->V; j++)
        {
            cm->disks[d].storage[j].object_id = 0;
            cm->disks[d].storage[j].block_id = 0;
        }
        cm->disks[d].initBlockGroup(block_group_num, cm->V);
    }

    // 初始化对象和请求（数组下标0未用）
    for (int i = 0; i < MAX_OBJECT_NUM; i++)
    {
        cm->objects[i].size = 0;
        cm->objects[i].tag = 0;
        cm->objects[i].last_request_point = 0;
        cm->objects[i].is_delete = false;
        for (int rep = 1; rep <= REP_NUM; rep++)
        {
            cm->objects[i].unit[rep] = nullptr;
        }
        cm->objects[i].block_read_status = nullptr;
    }
    for (int i = 0; i < MAX_REQUEST_NUM; i++)
    {
        cm->requests[i].is_done = false;
    }

    // 模拟 T + EXTRA_TIME 个时间片事件

    for (int t = 1; t <= T + EXTRA_TIME; t++)
    {
        handle_timestamp(cm);
        delete_action(cm);
        write_action(cm);
        read_action(cm);
    }

    clean(cm);

    // 文件路径
    const char* file_path = "res1.txt";

    // 打开文件，使用 std::ios::app 模式以追加方式写入
    std::ofstream file(file_path, std::ios::app);

    // 检查文件是否成功打开
    if (!file.is_open()) {
        std::cerr << "无法打开文件: " << file_path << std::endl;
        return 1;
    }

    // 写入内容
    for(auto it:siz){
        file<<it<<endl;
    }

    // 关闭文件
    file.close();

    std::cout << "内容已追加到文件: " << file_path << std::endl;
    return 0;
}
