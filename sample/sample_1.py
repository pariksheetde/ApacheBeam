import apache_beam as beam

p2 = beam.Pipeline()

lines = (
            p2
            | beam.Create([
               'Using create transform ',
               'to generate in memory data ',
               'This is 3rd line ',
               'Thanks '])
     
            | beam.io.WriteToText('D:/DataSet/OutputDataset/beam/sample_1')
          )
p2.run()  