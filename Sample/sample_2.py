import apache_beam as beam

p3 = beam.Pipeline()

lines1 = (
    p3
    | beam.Create([1,2,3,4,5,6,7,8,9])       
           | beam.io.WriteToText("D:/DataSet/OutputDataset/beam/sample_2")
         )
p3.run()
