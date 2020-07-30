import datetime
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import tensorflow as tf
from tensorflow.keras import layers, models, Model, callbacks
import base_param_addf as base_param
from tensorflow.keras import backend as K
import os
import sys
from datetime import datetime, date, timedelta
from os.path import isfile, join

select_dict = {}
for feature in base_param.ADDF_FEATURE_KEYS + base_param.SUCCESS_FEATURE_KEYS + base_param.CATEGORICAL_FEATURE_KEYS + base_param.NUMERIC_FEATURE_KEYS + base_param.HASH_FEATURE_KEYS + base_param.LABEL_KEY:
    select_dict[feature] = base_param.COLUMNS.index(feature)
select_dict = sorted(select_dict.items(),
                     key=lambda item: item[1], reverse=False)
feature_list, select_cols = zip(*select_dict)
n_inputs = len(select_cols) - 1


def dataPreprocess(line):
    defs = []
    for feature in feature_list:
        if feature in base_param.CATEGORICAL_FEATURE_KEYS:
            defs.append(0)
        elif feature in base_param.NUMERIC_FEATURE_KEYS + base_param.SUCCESS_FEATURE_KEYS + base_param.ADDF_FEATURE_KEYS:
            defs.append(0.0)
        elif feature in base_param.HASH_FEATURE_KEYS:
            defs.append("0")
        elif feature in base_param.LABEL_KEY:
            defs.append(tf.constant([], dtype=tf.int32))
    print()
    fields = tf.io.decode_csv(line, record_defaults=defs,
                              select_cols=select_cols, field_delim='\t', na_value='\\N')
    features = dict(zip(feature_list, fields))
    label = {'pred_addf': features.pop(
        'addf_num'), 'pred_suc': features.pop('suc_num')}
    return features, label


def read_dataset(filepaths, shuffle=True, batch_size=20000, shuffle_buffer_size=100000, n_parse_threads=5):
    dataset = tf.data.TextLineDataset(filepaths)
    dataset = dataset.map(dataPreprocess, num_parallel_calls=n_parse_threads)
    if shuffle:
        dataset = dataset.shuffle(buffer_size=shuffle_buffer_size)
        return dataset.batch(batch_size).prefetch(1)
    else:
        return dataset.batch(batch_size)


def feature_process():
    dnn_feature_columns = {}
    addfScore_feature_columns = {}
    successScore_feature_columns = {}
    inputs = {}

    for con_col in base_param.NUMERIC_FEATURE_KEYS:
        dnn_feature_columns[con_col] = tf.feature_column.numeric_column(
            con_col)
        inputs[con_col] = layers.Input(name=con_col, shape=(), dtype='float32')

    for con_col in base_param.SUCCESS_FEATURE_KEYS:
        successScore_feature_columns[con_col] = tf.feature_column.numeric_column(
            con_col)
        inputs[con_col] = layers.Input(name=con_col, shape=(), dtype='float32')

    for con_col in base_param.ADDF_FEATURE_KEYS:
        if con_col in ['avg_addf_score']:
            addfScore_feature_columns[con_col] = tf.feature_column.bucketized_column(
                tf.feature_column.numeric_column(con_col), boundaries=base_param.BUCKET_DICT[con_col])
        else:
            addfScore_feature_columns[con_col] = tf.feature_column.numeric_column(
                con_col)
            if con_col in base_param.BUCKET_FEATURE_KEYS:
                addfScore_feature_columns[con_col+'_bucket'] = tf.feature_column.bucketized_column(
                    tf.feature_column.numeric_column(con_col), boundaries=base_param.BUCKET_DICT[con_col])

        inputs[con_col] = layers.Input(name=con_col, shape=(), dtype='float32')

    for cate_col in base_param.CATEGORICAL_FEATURE_KEYS:
        dnn_feature_columns[cate_col] = tf.feature_column.indicator_column(
            tf.feature_column.categorical_column_with_vocabulary_list(
                key=cate_col, vocabulary_list=base_param.CATEGORY_DICT[cate_col]))
        inputs[cate_col] = layers.Input(name=cate_col, shape=(), dtype='int32')

    for hash_col in base_param.HASH_FEATURE_KEYS:
        dnn_feature_columns[hash_col] = tf.feature_column.embedding_column(
            tf.feature_column.categorical_column_with_hash_bucket(hash_col, base_param.HASH_NUM[hash_col]), 32)
        inputs[hash_col] = layers.Input(
            name=hash_col, shape=(), dtype='string')

    return dnn_feature_columns, addfScore_feature_columns, successScore_feature_columns, inputs


def custom_mse(y_true, y_pred):
    cutOff = K.constant(15)
    threshold = K.constant(10)
    y_pred = tf.where(y_pred > cutOff, cutOff, y_pred)
    y_true = K.cast(y_true, 'float32')
    # loss_weight = tf.where(y_true > threshold, 1.0, 1.0)
    # return K.mean(K.square(tf.multiply(loss_weight,(y_pred - y_true))), axis=-1)
    return K.mean(K.square(y_pred - y_true), axis=-1)


def stage_metric(y_true, y_pred):
    threshold_2 = K.constant(10)
    threshold_1 = K.constant(5)
    return K.mean(K.square(y_pred - y_true), axis=-1)

# def wide_and_deep_Regression(inputs, addfScore_feature_columns, successScore_feature_columns, dnn_feature_columns, dnn_hidden_units):
#     deep = layers.DenseFeatures(dnn_feature_columns, name='deep_inputs')(inputs)
#     for layerno, numnodes in enumerate(dnn_hidden_units):
#         deep = layers.Dense(numnodes, activation='relu', name='dnn_{}'.format(layerno+1))(deep)
#     addf = layers.DenseFeatures(addfScore_feature_columns, name='addf_inputs')(inputs)
#     success = layers.DenseFeatures(successScore_feature_columns, name='success_inputs')(inputs)
#     addf_deep = layers.concatenate([deep, addf], name='addf_deep')
#     success_deep = layers.concatenate([deep, success], name='success_deep')
#     output_addf = layers.Dense(1, activation='relu', name='pred_addf')(addf_deep)
#     output_suc = layers.Dense(1, activation='relu', name='pred_suc')(success_deep)
#     model = Model(inputs, outputs = [output_addf, output_suc])

#     lossWeights = {"pred_addf": 1.0, "pred_suc": 1.0}

#     model.compile(optimizer='rmsprop',loss='mse',loss_weights=lossWeights, metrics=[tf.keras.metrics.RootMeanSquaredError(), 'mae', custom_mse])
#     return model


def addf_wide_and_deep_Regression(inputs, addfScore_feature_columns, dnn_feature_columns, dnn_hidden_units):
    deep = layers.DenseFeatures(
        dnn_feature_columns, name='deep_inputs')(inputs)
    for layerno, numnodes in enumerate(dnn_hidden_units):
        deep = layers.Dense(numnodes, activation='relu',
                            name='dnn_{}'.format(layerno+1))(deep)
    addf = layers.DenseFeatures(
        addfScore_feature_columns, name='addf_inputs')(inputs)
    addf_deep = layers.concatenate([deep, addf], name='addf_deep')
    output_addf = layers.Dense(
        1, activation='relu', name='pred_addf')(addf_deep)
    model = Model(inputs, outputs=[output_addf])
    model.compile(optimizer='rmsprop', loss='mse', metrics=[
                  tf.keras.metrics.RootMeanSquaredError(), 'mae', custom_mse])
    return model


def printlog(info):
    nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n"+"=========="*8 + "%s" % nowtime)
    print(info+'...\n')


def train(train_begin_date, train_end_date, test_date):
    printlog("step1: prepare dataset...")
    FilePath = '/dataset/fandi/ctr_wk_data/'
    train_begin_datetime = datetime.strptime(train_begin_date, "%Y-%m-%d")
    train_end_datetime = datetime.strptime(train_end_date, "%Y-%m-%d")
    date_list = []
    while(train_begin_datetime <= train_end_datetime):
        train_begin_datetime = train_begin_datetime + +timedelta(days=1)
        date_list.append(train_begin_datetime.strftime("%Y-%m-%d")+'.merge')
    trainFilePaths = [FilePath +
                      f for f in date_list if isfile(join(FilePath, f))]
    print(trainFilePaths)
    valFilePaths = FilePath+'/{0}.merge'.format(test_date)
    train_ds = read_dataset(trainFilePaths)
    val_ds = read_dataset(valFilePaths, shuffle=False)

    printlog("step2: make feature columns...")
    dnn_feature_columns, addfScore_feature_columns, successScore_feature_columns, inputs = feature_process()

    printlog("step3: define model")
    # checkpoint_cb = "./modelSavePath/training/keras_model.h5"
    checkpoint_cb = "./modelSavePath/training/wkd_addf_model.h5"

    DNN_HIDDEN_UNITS = [64, 32, 16, 8]
    model = addf_wide_and_deep_Regression(inputs, addfScore_feature_columns.values(
    ), dnn_feature_columns.values(), DNN_HIDDEN_UNITS)

    if os.path.exists(checkpoint_cb):
        print("load model")
        model = models.load_model(checkpoint_cb, custom_objects={
                                  'custom_mse': custom_mse})

    checkpoint_cb = callbacks.ModelCheckpoint(
        checkpoint_cb, monitor='loss', verbose=1, save_best_only=True, mode='min')

    printlog("step4: train model with current checkpoint...")
    model.fit(train_ds, validation_data=val_ds,
              epochs=10, callbacks=[checkpoint_cb])

    printlog("step5: save model...")
    model_version = test_date
    model_name = "./modelSavePath/predictNum"
    model_path = os.path.join(model_name, model_version)
    tf.saved_model.save(model, model_path)


if __name__ == '__main__':
    train_begin_date = sys.argv[1]
    train_end_date = sys.argv[2]
    test_date = sys.argv[3]
    train(train_begin_date, train_end_date, test_date)
