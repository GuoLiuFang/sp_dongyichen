# encoding:utf-8
import sys
import jieba


def pre_fenci(input):
    path_tmp = "fenci_result.txt"
    fenci_result = open(path_tmp, mode='wa+')
    for line in open(input).readlines():
        line_cut_result = jieba.cut(line)
        line_sep_by_white_space = ' '.join(line_cut_result).encode(encoding='utf-8')
        fenci_result.write(line_sep_by_white_space)
    return path_tmp


if __name__ == '__main__':
    pre_fenci(sys.argv[1])
